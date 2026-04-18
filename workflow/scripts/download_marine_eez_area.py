"""Download data from the Marineregions database.

https://www.marineregions.org/
"""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pandas as pd
import requests
from matplotlib import pyplot as plt

if TYPE_CHECKING:
    snakemake: Any

WFS_BASE = "https://geo.vliz.be/geoserver/MarineRegions/wfs"
WFS_VERSION = "2.0.0"


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


def get_eez_by_cql(
    cql_filter: str, *, srs_name: str = "EPSG:4326", timeout: int = 180
) -> gpd.GeoDataFrame | None:
    """Fetch EEZ polygons using a raw WFS CQL filter."""
    params = {
        "service": "WFS",
        "version": WFS_VERSION,
        "request": "GetFeature",
        "typeNames": "eez",
        "outputFormat": "application/json",
        "cql_filter": cql_filter,
        "srsName": srs_name,
    }

    response = requests.get(WFS_BASE, params=params, timeout=timeout)

    if response.status_code >= 400:
        # Attempt to explain the failure, otherwise just raise status.
        _raise_for_geoserver_exception(response)
        response.raise_for_status()
    # Attempt to explain 200 failures too.
    _raise_for_geoserver_exception(response)

    data = response.json()
    if "features" not in data:
        raise RuntimeError(
            f"Unexpected JSON payload (no 'features'). First keys: {list(data)[:20]}"
        )

    result = None
    if data["features"]:
        # Let geopandas build the frame, CRS will match request
        result = gpd.GeoDataFrame.from_features(data["features"], crs=srs_name)
        if result.empty:
            result = None
    return result


def get_country_eez_by_iso3(
    iso3: str,
    *,
    id_col: str = "iso_ter1",
    srs_name: str = "EPSG:4326",
    timeout: int = 180,
) -> gpd.GeoDataFrame | None:
    """Fetch EEZ polygons by ISO3 code using WFS + CQL."""
    return get_eez_by_cql(f"{id_col}='{iso3}'", srs_name=srs_name, timeout=timeout)


def get_country_eez_by_mrgid(
    mrgid: int, *, srs_name: str = "EPSG:4326", timeout: int = 180
) -> gpd.GeoDataFrame | None:
    """Fetch EEZ polygons by Marine Regions ID."""
    return get_eez_by_cql(f"mrgid={mrgid}", srs_name=srs_name, timeout=timeout)


def transform_to_clio(gdf: gpd.GeoDataFrame, country_id: str) -> gpd.GeoDataFrame:
    """Transform the MarineRegions dataset for better clio compatibility.

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
            }
        )
        # Remove cases without territorial ISO code
        standardised = standardised[~standardised["country_id"].isna()]
        standardised = _schemas.ShapesSchema.validate(standardised)
        # Extra: identify contested areas and potential attribution conflicts
        standardised["contested"] = gdf["pol_type"].apply(
            lambda x: x in ["Joint regime", "Overlapping claim"]
        )
    else:
        standardised = gpd.GeoDataFrame(
            columns=_schemas.ShapesSchema.to_schema().columns
        )
    return standardised


def plot(gdf: gpd.GeoDataFrame, country: str):
    """Simple plot of the shape."""
    fig, ax = plt.subplots(layout="constrained")
    if gdf.empty:
        ax.set_title(f"{country!r} has no EEZ")
    else:
        gdf.plot("contested", ax=ax, legend=True, legend_kwds={"title": "contested", "bbox_to_anchor":(1, 1)})
        ax.set_title(f"{country!r}: EEZ")
    ax.set_axis_off()
    return fig, ax


def main() -> None:
    """Main snakemake process."""
    iso3 = snakemake.wildcards.country
    extra_eez = snakemake.params.extra_eez
    if not isinstance(extra_eez, list):
        extra_eez = [extra_eez]

    eez_gdfs = [get_country_eez_by_iso3(iso3)]
    for mrgid in extra_eez:
        extra_gdf = get_country_eez_by_mrgid(int(mrgid))
        if extra_gdf is None:
            raise RuntimeError(f"Configured extra_eez {mrgid!r} returned no features")
        eez_gdfs.append(extra_gdf)

    combined_gdf = pd.concat([i for i in eez_gdfs if i is not None])

    gdf = transform_to_clio(combined_gdf, iso3)
    gdf.to_parquet(snakemake.output.path)

    fig, _ = plot(gdf, iso3)
    fig.savefig(snakemake.output.plot, bbox_inches="tight", dpi=200)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
