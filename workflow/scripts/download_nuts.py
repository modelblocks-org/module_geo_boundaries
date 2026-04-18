"""NUTS download functionality."""

import sys
from typing import TYPE_CHECKING, Any

import geopandas as gpd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")
URL = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/gpkg/NUTS_RG_{resolution}_{year}_{epsg}_LEVL_{level}.gpkg"


def download_nuts_version(year: int, resolution: str, level: str, epsg: str, path: str):
    """Download an aggregated NUTS datafile for the requested configuration."""
    gdf = gpd.read_file(
        URL.format(year=year, resolution=resolution, epsg=epsg, level=level)
    )
    assert gdf.crs.equals(epsg)
    gdf.to_parquet(path)


if __name__ == "__main__":
    download_nuts_version(
        year=snakemake.wildcards.year,
        resolution=snakemake.wildcards.resolution,
        level=snakemake.wildcards.subtype,
        epsg=snakemake.params.epsg,
        path=snakemake.output.path,
    )
