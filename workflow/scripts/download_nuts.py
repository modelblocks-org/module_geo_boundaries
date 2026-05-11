"""NUTS download functionality."""

import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import geopandas as gpd
from _utils import DownloadTimeouts, download_file

if TYPE_CHECKING:
    snakemake: Any

URL = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/gpkg/NUTS_RG_{resolution}_{year}_{crs}_LEVL_{level}.gpkg"
NUTS_CRS = 3035


def download_nuts_version(
    year: int, resolution: str, level: str, timeouts: DownloadTimeouts
) -> gpd.GeoDataFrame:
    """Download an aggregated NUTS datafile for the requested configuration."""
    url = URL.format(year=year, resolution=resolution, crs=NUTS_CRS, level=level)
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir) / "download.gpkg"
        download_file(url, tmp_path, timeouts)
        gdf = gpd.read_file(tmp_path)

    if not gdf.crs.equals(NUTS_CRS):
        raise ValueError(f"NUTS in unexpected CRS: got {gdf.crs}, expected {NUTS_CRS}.")
    return gdf


def main():
    """Main snakemake process."""
    gdf = download_nuts_version(
        year=snakemake.wildcards.year,
        resolution=snakemake.wildcards.resolution,
        level=snakemake.wildcards.subtype,
        timeouts=DownloadTimeouts(**snakemake.params.timeouts),
    )
    gdf.to_parquet(snakemake.output.path)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
