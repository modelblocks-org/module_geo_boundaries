"""Build one configured EEZ dataset from cached MarineRegions downloads."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pandas as pd
from _utils import CRS_MARINE_REGIONS

if TYPE_CHECKING:
    snakemake: Any


def build_eez(country: str, paths: list[str]) -> gpd.GeoDataFrame:
    """Combine a country EEZ file with optional extra MarineRegions ID files."""
    frames = [
        _schemas.EEZSchema.validate(gpd.read_parquet(path)).to_crs(CRS_MARINE_REGIONS)
        for path in paths
    ]
    frames = [frame for frame in frames if not frame.empty]

    if frames:
        combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))
        combined["country_id"] = country
        combined["shape_id"] = combined["parent_id"].apply(
            lambda parent_id: "_".join([country, "marineregions", str(parent_id)])
        )
    else:
        combined = gpd.GeoDataFrame(
            columns=_schemas.EEZSchema.to_schema().columns,
            geometry="geometry",
            crs=CRS_MARINE_REGIONS,
        )

    return _schemas.EEZSchema.validate(combined)


def main() -> None:
    """Main snakemake process."""
    input_paths = [snakemake.input.country, *snakemake.input.extra]
    gdf = build_eez(snakemake.wildcards.country, input_paths)
    gdf.to_parquet(snakemake.output.path)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
