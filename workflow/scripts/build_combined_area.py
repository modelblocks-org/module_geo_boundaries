"""Combine country shapes and marine regions into one harmonized dataset."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from pyproj import CRS

if TYPE_CHECKING:
    snakemake: Any


def plot_combined_area(combined_file: str, path: str, crs: str):
    """Generate a nice figure of the resulting file."""
    gdf = gpd.read_parquet(combined_file).to_crs(crs)
    fig, ax = plt.subplots(figsize=(7, 7), layout="constrained")
    ax = gdf.plot(ax=ax, column="shape_class", legend=False)
    ax.set(xticks=[], yticks=[], xlabel="", ylabel="")
    ax.set_title("Combined regions")
    fig.savefig(path, dpi=200, bbox_inches="tight")


def _remove_overlaps(gdf: gpd.GeoDataFrame, projected_crs: str) -> gpd.GeoDataFrame:
    """Remove overlaps between regional shapes and clip shapes using neighbors.

    Args:
        gdf (gpd.GeoDataFrame): dataframe with regional shapes.
        projected_crs (str): CRS to use. Must be projected.

    Returns:
        gpd.GeoDataFrame: dataframe in the projected CRS.
    """
    # Buffering requires a projected CRS
    assert CRS(projected_crs).is_projected
    projected = gdf.to_crs(projected_crs)

    # A buffer of 0 resolves floating point mismatches that occur during geospatial operations
    buffered = projected.buffer(0)

    for index, row in projected.iterrows():
        minx, miny, maxx, maxy = row.geometry.bounds

        # Find neighbouring regions and only use those for the calculation
        neighbours = buffered.cx[minx:maxx, miny:maxy]
        neighbours = neighbours[neighbours.index != index]
        new_geometry = row["geometry"].difference(neighbours.union_all())

        if not new_geometry.is_valid:
            new_geometry = new_geometry.buffer(0)
            assert new_geometry.is_valid, "Invalid bowties could not be corrected."
        assert new_geometry is not None
        projected.loc[index, "geometry"] = new_geometry
    return projected


def _combine_shapes(
    country_files: list[str], eez: gpd.GeoDataFrame, geographic_crs: str
) -> gpd.GeoDataFrame:
    """Merge all countries and maritime boundaries into one file.

    Args:
        country_files (list[str]): List of standardised country files to combine.
        eez (gpd.GeoDataFrame): Standardised EEZ shapes.
        geographic_crs (str): CRS to use. Must be geographic.

    Raises:
        ValueError: Country file is not a unique country.

    Returns:
        gpd.GeoDataFrame: Combined dataframe using the given CRS.
    """
    assert CRS(geographic_crs).is_geographic

    frames = []
    if not eez.empty:
        eez = eez.to_crs(geographic_crs)
        # No contested or ambiguous EEZs
        eez = eez[eez["contested"].eq(False)].drop(columns="contested")

    # Combine land and marine boundary for each country
    for file in country_files:
        # Fetch the country file and ensure crs is compatible
        country_land = gpd.read_parquet(file).to_crs(geographic_crs)
        country_id = country_land["country_id"].unique()
        if len(country_id) != 1:
            raise ValueError(
                f"Country file {file} should be a single country. Found {country_id}."
            )
        country_id = country_id[0]

        country_marine = eez[eez["country_id"] == country_id]
        if not country_marine.empty:
            marine_geom = country_marine.geometry.union_all()
            # clip land with maritime boundaries
            country_land = country_land.copy()
            country_land.geometry = country_land.geometry.difference(marine_geom)

            # only add uncontested maritime boundaries
            frames.extend([country_land, country_marine])
        else:
            frames.append(country_land)

    combined = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True), crs=geographic_crs
    )
    return combined


def _combine_eez(eez_files: list[str]) -> gpd.GeoDataFrame:
    """Merge all eez files into a big dataset to be processed further."""
    frames: list[gpd.GeoDataFrame] = []
    for path in eez_files:
        gdf = gpd.read_parquet(path)
        frames.append(gdf)
    return pd.concat(frames, axis="rows", ignore_index=True)


def build_combined_area(
    country_files: list[str],
    marine_files: list[str],
    crs: dict[str, str],
    combined_file: str,
) -> None:
    """Create a single file with the requested geographical scope."""
    eezs = _combine_eez(marine_files)
    combined = _combine_shapes(country_files, eezs, crs["geographic"])
    combined = _remove_overlaps(combined, crs["projected"])

    combined = combined.to_crs(crs["geographic"])
    # A buffer of 0 resolves floating point mismatches that occur during CRS conversion
    combined["geometry"] = combined.buffer(0)
    combined = _schemas.ShapesSchema.validate(combined)
    combined.reset_index(drop=True).to_parquet(combined_file)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    build_combined_area(
        country_files=snakemake.input.countries,
        marine_files=snakemake.input.marine,
        crs=snakemake.params.crs,
        combined_file=snakemake.output.combined,
    )
    plot_combined_area(
        combined_file=snakemake.output.combined,
        path=snakemake.output.plot,
        crs=snakemake.params.crs["projected"],
    )
