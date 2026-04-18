"""Combine country shapes and marine regions into one harmonized dataset."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pyproj import CRS

if TYPE_CHECKING:
    snakemake: Any


def combine_shapes(
    land: gpd.GeoDataFrame, maritime: gpd.GeoDataFrame, geo_crs: str
) -> gpd.GeoDataFrame:
    """Combine land and marine shapes."""
    crs = CRS.from_user_input(geo_crs)
    if not crs.is_geographic:
        raise ValueError(f"CRS must be geographic. Got {geo_crs!r}.")

    combined = land.copy().to_crs(geo_crs)
    if not maritime.empty:
        # remove contested zones and clip to give priority to maritime polygons
        eez = maritime.copy().to_crs(geo_crs)
        eez = eez[eez["contested"].eq(False)].drop(columns="contested")
        combined.geometry = combined.geometry.difference(eez.geometry.union_all())
        combined = pd.concat([combined, eez], ignore_index=True)

    # Resolve floating point mismatches that occur during CRS conversion
    combined.geometry = combined.geometry.buffer(0)
    return combined


def plot_combined_area(combined: gpd.GeoDataFrame, crs: str) -> tuple[Figure, Axes]:
    """Generate a nice figure of the resulting file."""
    gdf = combined.copy().to_crs(crs)
    fig, ax = plt.subplots(figsize=(7, 7), layout="constrained")
    ax = gdf.plot(ax=ax, column="shape_class", legend=False)
    ax.set(xticks=[], yticks=[], xlabel="", ylabel="")

    return fig, ax


def main() -> None:
    """Main snakemake process."""
    crs = snakemake.params.crs
    country = snakemake.wildcards.country
    land = _schemas.ShapesSchema.validate(gpd.read_parquet(snakemake.input.land))
    maritime = _schemas.EEZSchema.validate(gpd.read_parquet(snakemake.input.maritime))

    country_ids = set(land["country_id"]) | set(maritime["country_id"])
    if set(country_ids) - set([country]):
        raise ValueError(
            f"Country processing mismatch for {country!r}. Found {country_ids!r}."
        )

    shapes = combine_shapes(land, maritime, crs["geographic"])
    _schemas.ShapesSchema.validate(shapes).to_parquet(snakemake.output.country)

    fig, _ = plot_combined_area(shapes, crs["projected"])
    fig.suptitle(f"{country} shapes")
    fig.savefig(snakemake.output.plot, dpi=200, bbox_inches="tight")


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
