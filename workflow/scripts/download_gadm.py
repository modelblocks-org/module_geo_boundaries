"""Download data from the GADM database.

Built for version 4.1 of the dataset.
https://gadm.org/index.html
"""

import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import geopandas as gpd
from _utils import DownloadTimeouts, download_file

if TYPE_CHECKING:
    snakemake: Any


GADM_URL = (
    "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_{country}_{subtype}.json{zip}"
)
GADM_CRS = "EPSG:4326"


def download_country_gadm(country: str, subtype: str, timeouts: DownloadTimeouts) -> gpd.GeoDataFrame:
    """Attempts to download country GADM data in .json or zipped json."""
    last_error: Exception | None = None

    for zip_ext in (".zip", ""):
        url = GADM_URL.format(country=country, subtype=subtype, zip=zip_ext)
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / f"download.json{zip_ext}"

                download_file(url, tmp_path, timeouts)
                gdf = gpd.read_file(tmp_path)
                if gdf.empty:
                    raise RuntimeError(f"Downloaded empty GADM file from {url!r}.")
                return gdf.to_crs(GADM_CRS)

        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        f"Could not fetch GADM request for {country!r}:{subtype!r}."
    ) from last_error


def main():
    """Main snakemake process."""
    timeouts = DownloadTimeouts(**snakemake.params.timeouts)
    country = download_country_gadm(
        snakemake.wildcards.country, snakemake.wildcards.subtype, timeouts
    )
    country.to_parquet(snakemake.output.path)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
