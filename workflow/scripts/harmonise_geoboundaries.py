"""Harmonise a geoBoundaries country dataset into a cross-compatible shape.

NOTE: there is not enough metadata to filter contested regions in this dataset!
"""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd

if TYPE_CHECKING:
    snakemake: Any


def harmonise_geoboundaries(
    input_path: str, country_id: str, release_type: str
) -> gpd.GeoDataFrame:
    """Harmonise a geoBoundaries dataset including metadata."""
    gdf = gpd.read_parquet(input_path)

    shape_type = gdf["shapeType"].astype("string")
    shape_id = gdf["shapeID"].astype("string")

    harmonised = gpd.GeoDataFrame(
        {
            "shape_id": f"{release_type}_{country_id}_" + shape_type + "_" + shape_id,
            "country_id": country_id,
            "shape_class": "land",
            "geometry": gdf["geometry"],
            "parent": "geoboundaries",
            "parent_subtype": f"{release_type}_" + shape_type,
            "parent_id": shape_id,
            "parent_name": gdf["shapeName"],
        },
        geometry="geometry",
        crs=gdf.crs,
    ).reset_index(drop=True)

    return _schemas.ShapesSchema.validate(harmonised)


def main():
    """Main snakemake process."""
    gdf = harmonise_geoboundaries(
        input_path=snakemake.input.raw,
        country_id=snakemake.wildcards.country,
        release_type=snakemake.wildcards.release_type,
    )
    gdf.to_parquet(snakemake.output.path)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
