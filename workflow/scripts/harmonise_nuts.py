"""Download a NUTS map from the GISCO website."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pycountry

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")

UNIQUE_ISO3_TO_NUTS = {
    "GRC": "EL",  # Greece
    "GBR": "UK",  # United Kingdom
}


def _iso_a3_to_nuts(code):
    """Obtain NUTS code for the requested country, handling special cases."""
    nuts = UNIQUE_ISO3_TO_NUTS.get(code, None)
    if nuts is None:
        nuts = pycountry.countries.get(alpha_3=code).alpha_2
    return nuts


def standardise_country_nuts(
    raw_file: str, country_id: str, year: int, subtype: str, output_path: str
):
    """Extract country data from a NUTS file and standardise it.

    Args:
        raw_file (str): NUTS parquet file with raw data.
        country_id (str): ISO alpha 3 country code.
        year (int): NUTS year version.
        subtype (str): Disaggregation level of the file (i.e., 0, 1, 2...).
        output_path (str): output path for the standardised file.
    """
    nuts_version = f"nuts{year}"
    nuts_id = _iso_a3_to_nuts(country_id)

    nuts_gdf = gpd.read_parquet(raw_file)
    nuts_gdf = nuts_gdf[nuts_gdf["CNTR_CODE"] == nuts_id]
    standardised_gdf = gpd.GeoDataFrame(
        {
            "shape_id": nuts_gdf["NUTS_ID"].apply(
                lambda x: country_id + "_" + nuts_version + "_" + x
            ),
            "country_id": country_id,
            "shape_class": "land",
            "geometry": nuts_gdf["geometry"],
            "parent": "nuts",
            "parent_subtype": nuts_gdf["LEVL_CODE"].astype(str),
            "parent_id": nuts_gdf["NUTS_ID"],
            "parent_name": nuts_gdf["NUTS_NAME"],
        }
    )
    standardised_gdf = _schemas.ShapesSchema.validate(standardised_gdf)
    standardised_gdf.to_parquet(output_path)


if __name__ == "__main__":
    standardise_country_nuts(
        raw_file=snakemake.input.raw,
        country_id=snakemake.wildcards.country,
        year=snakemake.wildcards.year,
        subtype=snakemake.wildcards.subtype,
        output_path=snakemake.output.path,
    )
