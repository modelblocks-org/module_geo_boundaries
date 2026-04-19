"""Combine country shapes and marine regions into one harmonized dataset."""

import math
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pyproj import CRS
from shapely import voronoi_polygons
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    Point,
)
from shapely.geometry.base import BaseGeometry

if TYPE_CHECKING:
    snakemake: Any

ROUND_DECIMALS: int = 3


def _iter_lines(geom: BaseGeometry) -> Iterator[LineString]:
    if geom.is_empty:
        return
    if isinstance(geom, LineString):
        yield geom
        return
    if isinstance(geom, MultiLineString):
        yield from geom.geoms
        return
    if isinstance(geom, GeometryCollection):
        for part in geom.geoms:
            yield from _iter_lines(part)


def _sample_line_midpoints(line: LineString, spacing: float) -> list[Point]:
    if line.length == 0:
        return []
    n = max(1, math.ceil(line.length / spacing))
    return [line.interpolate((i + 0.5) * line.length / n) for i in range(n)]


def _split_one_maritime(
    maritime_row,
    land: gpd.GeoDataFrame,
    *,
    crs: int | str,
    sample_spacing: float,
    coverage_area_tolerance: float,
) -> gpd.GeoDataFrame:
    if land.empty:
        raise ValueError(
            f"No land shapes found for country_id={maritime_row.country_id!r}."
        )

    # Sample through the shoreline
    boundary = maritime_row.geometry.boundary
    candidate_land = land.iloc[land.sindex.query(boundary, predicate="intersects")]

    seeds: list[dict[str, object]] = []
    for land_row in candidate_land.itertuples(index=False):
        shoreline = land_row.geometry.boundary.intersection(boundary)

        for line in _iter_lines(shoreline):
            for point in _sample_line_midpoints(line, sample_spacing):
                seeds.append(
                    {"assigned_shape_id": land_row.shape_id, "geometry": point}
                )
    if not seeds:
        raise ValueError(
            f"No touching shoreline regions found for maritime shape {maritime_row.shape_id!r}."
        )
    seed_gdf = gpd.GeoDataFrame(seeds, geometry="geometry", crs=crs)

    # Drop seeds too close to each other
    xy = seed_gdf.geometry.map(
        lambda p: (round(p.x, ROUND_DECIMALS), round(p.y, ROUND_DECIMALS))
    )
    seed_gdf = seed_gdf.loc[~xy.duplicated()].reset_index(drop=True)

    base_row = maritime_row._asdict()
    assigned_ids = seed_gdf["assigned_shape_id"].unique()
    if len(assigned_ids) == 1:
        # Nothing to do
        pieces = gpd.GeoDataFrame([base_row.copy()], geometry="geometry", crs=crs)
    else:
        # Run Voronoi
        cells = gpd.GeoDataFrame(
            geometry=list(
                voronoi_polygons(
                    MultiPoint(list(seed_gdf.geometry)),
                    extend_to=maritime_row.geometry.envelope.buffer(sample_spacing * 2),
                ).geoms
            ),
            crs=crs,
        )
        # Identify where each voronoi cell belongs
        cells = gpd.sjoin(
            cells,
            seed_gdf[["assigned_shape_id", "geometry"]],
            how="left",
            predicate="contains",
        ).drop_duplicates()
        if cells["assigned_shape_id"].isna().any():
            raise RuntimeError(
                f"Could not assign every Voronoi cell for maritime shape {maritime_row.shape_id!r}."
            )

        # Keep only cells on the maritime area and test for completeness
        cells["geometry"] = cells.geometry.intersection(maritime_row.geometry)
        cells = cells.loc[~cells.geometry.is_empty & (cells.geometry.area > 0)]
        pieces = (
            cells[["assigned_shape_id", "geometry"]]
            .dissolve(by="assigned_shape_id", as_index=False)
            .rename(columns={"assigned_shape_id": "_assigned_shape_id"})
        )
        uncovered_area = maritime_row.geometry.difference(
            pieces.geometry.union_all()
        ).area
        if uncovered_area > coverage_area_tolerance:
            raise RuntimeError(
                f"Voronoi split did not fully cover maritime shape {maritime_row.shape_id!r}. "
                f"Uncovered area: {uncovered_area}"
            )

        # Re-apply row values, keeping the new geometry and a new id
        piece_rows = []
        for assigned_id, geometry in zip(
            pieces["_assigned_shape_id"], pieces.geometry, strict=True
        ):
            piece = base_row.copy()
            piece["shape_id"] = f"{maritime_row.shape_id}_to_{assigned_id}"
            piece["geometry"] = geometry
            piece_rows.append(piece)
        pieces = gpd.GeoDataFrame(piece_rows, geometry="geometry", crs=crs)

    return pieces


def split_maritime_by_shoreline_voronoi(
    shapes: gpd.GeoDataFrame,
    *,
    crs: dict[str, str],
    sample_spacing: float = 10_000.0,
    coverage_area_tolerance: float = 1.0,
) -> gpd.GeoDataFrame:
    """Split EEZ zones to fit shoreline land regions."""
    shapes = shapes.copy().to_crs(crs["projected"])

    land = shapes.loc[shapes["shape_class"].eq("land")]
    maritime = shapes.loc[shapes["shape_class"].eq("maritime")]

    if maritime.empty:
        return ValueError("Requested voronoi without maritime shapes.")

    split_maritime = [
        _split_one_maritime(
            maritime_row,
            land.loc[land["country_id"].eq(maritime_row.country_id)],
            sample_spacing=sample_spacing,
            coverage_area_tolerance=coverage_area_tolerance,
            crs=shapes.crs,
        )
        for maritime_row in maritime.itertuples(index=False)
    ]

    result = pd.concat([land, *split_maritime], ignore_index=True)

    return result.to_crs(crs["geographic"])


def combine_shapes(
    land: gpd.GeoDataFrame, maritime: gpd.GeoDataFrame, geo_crs: str
) -> gpd.GeoDataFrame:
    """Combine land and marine shapes."""
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
    fig, ax = plt.subplots(layout="constrained")
    gdf.boundary.plot(ax=ax, color="black", lw=0.5)
    ax = gdf.plot(ax=ax, column="shape_class", legend=False)
    ax.set(xticks=[], yticks=[], xlabel="", ylabel="")

    return fig, ax


def main() -> None:
    """Main snakemake process."""
    crs = snakemake.params.crs
    if not CRS.from_user_input(crs["projected"]).is_projected:
        raise ValueError(f"CRS must be projected. Got {crs['projected']!r}.")
    if not CRS.from_user_input(crs["geographic"]).is_geographic:
        raise ValueError(f"CRS must be geographic. Got {crs['geographic']!r}.")

    country = snakemake.wildcards.country
    land = _schemas.ShapesSchema.validate(gpd.read_parquet(snakemake.input.land))
    maritime = _schemas.EEZSchema.validate(gpd.read_parquet(snakemake.input.maritime))

    country_ids = set(land["country_id"]) | set(maritime["country_id"])
    if set(country_ids) - set([country]):
        raise ValueError(
            f"Country processing mismatch for {country!r}. Found {country_ids!r}."
        )

    shapes = combine_shapes(land, maritime, crs["geographic"])
    shapes = _schemas.ShapesSchema.validate(shapes)

    if not maritime.empty and snakemake.params.voronoi:
        shapes = split_maritime_by_shoreline_voronoi(shapes, crs=crs)
        shapes = _schemas.ShapesSchema.validate(shapes)
    shapes.to_parquet(snakemake.output.country)

    fig, _ = plot_combined_area(shapes, crs["projected"])
    fig.suptitle(f"{country} shapes")
    fig.savefig(snakemake.output.plot, dpi=200, bbox_inches="tight")


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
