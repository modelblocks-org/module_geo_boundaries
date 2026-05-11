"""Download data from the Marineregions database.

https://www.marineregions.org/
Constructs EEZ datasets per country
- In cases where no EEZ is available for a country an empty dataframe will be saved.
- Optionally, additional EEZs may be downloaded if specified in `extra_eez`.
    - These should result in an error if the code is invalid.
"""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import _utils
import geopandas as gpd
import pandas as pd
import requests
from matplotlib import pyplot as plt

if TYPE_CHECKING:
    snakemake: Any

WFS_BASE = "https://geo.vliz.be/geoserver/MarineRegions/wfs"
WFS_VERSION = "2.0.0"
CRS_MARINE_REGIONS = "EPSG:4326"


def _raise_for_geoserver_exception(response: requests.Response) -> None:
    """GeoServer sometimes returns OGC ExceptionReport as XML with 200 or 400.

    Raise a helpful error either way.
    """
    ct = (response.headers.get("Content-Type") or "").lower()
    # Try to catch xml or vague responses
    if "xml" in ct or response.content.lstrip().startswith(b"<"):
        text = response.text
        raise RuntimeError(
            "GeoServer returned an XML response (likely an OGC ExceptionReport), not GeoJSON.\n"
            f"status={response.status_code}\n"
            f"url={response.url}\n"
            f"content-type={response.headers.get('Content-Type')}\n"
            f"body_start={text[:1200]}"
        )


def _get_wfs_response(
    params: dict[str, str], timeouts: _utils.DownloadTimeouts
) -> requests.Response:
    """Call the MarineRegions WFS API, retrying transient request failures."""
    max_retries = timeouts.max_retries
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(
                WFS_BASE,
                params=params,
                timeout=timeouts.request_timeout,
            )

            if response.status_code in _utils.RETRY_STATUS_CODES:
                if attempt < max_retries:
                    _utils.retry_wait_seconds(attempt, timeouts.initial_retry_seconds)
                    continue
                _raise_for_geoserver_exception(response)
                response.raise_for_status()

            if response.status_code >= 400:
                _raise_for_geoserver_exception(response)
                response.raise_for_status()

            _raise_for_geoserver_exception(response)
            return response

        except _utils.RETRY_EXCEPTIONS as exc:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Failed MarineRegions WFS request after {max_retries} retries."
                ) from exc
            _utils.retry_wait_seconds(attempt, timeouts.initial_retry_seconds)

    raise RuntimeError(f"Failed MarineRegions WFS request after {max_retries} retries.")


def get_eez_by_cql(
    cql_filter: str, timeouts: _utils.DownloadTimeouts
) -> gpd.GeoDataFrame | None:
    """Fetch EEZ polygons using a raw WFS CQL filter."""
    params = {
        "service": "WFS",
        "version": WFS_VERSION,
        "request": "GetFeature",
        "typeNames": "eez",
        "outputFormat": "application/json",
        "cql_filter": cql_filter,
        "srsName": CRS_MARINE_REGIONS,
    }
    response = _get_wfs_response(params, timeouts)

    data = response.json()
    if "features" not in data:
        raise RuntimeError(
            f"Unexpected JSON payload from MarineRegions. First keys: {list(data)[:20]}"
        )

    result = None
    if data["features"]:
        # Let geopandas build the frame, CRS will match request
        result = gpd.GeoDataFrame.from_features(
            data["features"], crs=CRS_MARINE_REGIONS
        )
        if result.empty:
            result = None
    return result


def transform_to_schema(
    gdf: gpd.GeoDataFrame | None, country_id: str
) -> gpd.GeoDataFrame:
    """Transform the MarineRegions dataset for better compatibility.

    - Removes geopolitically contested areas
    - Adds common naming conventions.

    Args:
        gdf (gpd.GeoDataFrame): A marine regions geo-dataframe.
        country_id (str): ISO3 country id from the workflow.

    Returns:
        gpd.GeoDataFrame: standardised dataframe.
    """
    if gdf is not None:
        standardised = gpd.GeoDataFrame(
            {
                "shape_id": gdf["mrgid"].apply(
                    lambda mrgid: "_".join([country_id, "marineregions", str(mrgid)])
                ),
                "country_id": country_id,
                "shape_class": "maritime",
                "geometry": gdf["geometry"],
                "parent": "marineregions",
                "parent_subtype": "eez",
                "parent_id": gdf["mrgid"],
                "parent_name": gdf["geoname"],
                "contested": gdf["pol_type"].apply(
                    lambda x: x in ["Joint regime", "Overlapping claim"]
                ),
            }
        )
        # Remove cases without territorial ISO code
        standardised = standardised[~standardised["country_id"].isna()]
        standardised = _schemas.EEZSchema.validate(standardised)
    else:
        standardised = gpd.GeoDataFrame(columns=_schemas.EEZSchema.to_schema().columns)
    return standardised


def plot(gdf: gpd.GeoDataFrame, country: str):
    """Simple plot of the shape."""
    fig, ax = plt.subplots(layout="constrained")
    if gdf.empty:
        ax.text(
            0.5,
            0.5,
            f"{country!r} has no EEZ",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=24,
        )
        ax.set_axis_off()
    else:
        gdf.plot(
            "contested",
            ax=ax,
            legend=True,
            legend_kwds={
                "title": "contested",
                "bbox_to_anchor": (1, 1),
                "loc": "upper left",
            },
        )
    return fig, ax


def download_eezs(
    country: str, extra_eez: list[str], timeouts: _utils.DownloadTimeouts
) -> gpd.GeoDataFrame:
    """Download EEZ data as requested.

    If no EEZ exists for a country, the dataframe will be empty.
    """
    eez_gdfs: list[gpd.GeoDataFrame] = []

    iso3_eez = get_eez_by_cql(f"iso_ter1='{country}'", timeouts)
    if iso3_eez is not None:
        eez_gdfs.append(iso3_eez)

    for mrgid in extra_eez:
        extra_gdf = get_eez_by_cql(f"mrgid={int(mrgid)}", timeouts)
        if extra_gdf is None:
            raise RuntimeError(f"Configured extra_eez {mrgid!r} returned no features")
        eez_gdfs.append(extra_gdf)

    combined_gdf = None
    if eez_gdfs:
        combined_gdf = pd.concat(eez_gdfs, ignore_index=True)

    return transform_to_schema(combined_gdf, country)


def main() -> None:
    """Main snakemake process."""
    country = snakemake.wildcards.country
    extra_eez = snakemake.params.extra_eez

    timeouts = _utils.DownloadTimeouts(**snakemake.params.timeouts)
    if not isinstance(extra_eez, list):
        extra_eez = [extra_eez]

    gdf = download_eezs(country, extra_eez, timeouts)
    gdf.to_parquet(snakemake.output.path)

    fig, _ = plot(gdf, country)
    fig.savefig(snakemake.output.plot, bbox_inches="tight", dpi=200)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
