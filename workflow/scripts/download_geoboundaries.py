"""Download data from the geoBoundaries API.

https://www.geoboundaries.org/api.html
"""

import json
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import geopandas as gpd
from _utils import DownloadTimeouts, download_file

if TYPE_CHECKING:
    snakemake: Any


GEOBOUNDARIES_API_URL = (
    "https://www.geoboundaries.org/api/current/{release_type}/{country}/ADM{subtype}/"
)
GEOBOUNDARIES_CRS = "EPSG:4326"


def download_country_geoboundaries(
    country: str,
    subtype: str,
    release_type: str,
    timeouts: DownloadTimeouts,
) -> gpd.GeoDataFrame:
    """Download country data from geoBoundaries.

    Uses the current geoBoundaries API endpoint.
    The concrete dataset version returned in the metadata JSON response.
    """
    api_url = GEOBOUNDARIES_API_URL.format(
        release_type=release_type,
        country=country,
        subtype=subtype,
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        metadata_path = tmp_path / "metadata.json"
        download_file(api_url, metadata_path, timeouts)

        metadata = json.loads(metadata_path.read_text())
        geojson_url = metadata["gjDownloadURL"]

        geojson_path = tmp_path / "download.geojson"
        download_file(geojson_url, geojson_path, timeouts)

        gdf = gpd.read_file(geojson_path)
        if gdf.empty:
            raise RuntimeError(
                f"Downloaded empty geoBoundaries file from {geojson_url!r}."
            )

    return gdf.to_crs(GEOBOUNDARIES_CRS)


def main():
    """Main snakemake process."""
    timeouts = DownloadTimeouts(**snakemake.params.timeouts)

    country = download_country_geoboundaries(
        snakemake.wildcards.country,
        snakemake.wildcards.subtype,
        snakemake.wildcards.release_type,
        timeouts,
    )
    country.to_parquet(snakemake.output.path)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
