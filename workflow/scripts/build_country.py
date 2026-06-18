"""Combine country shapes and marine regions into one harmonized dataset."""

import math
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import _geo
import _schemas
import _utils
import geopandas as gpd
import pandas as pd
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
from shapely.ops import linemerge, unary_union

if TYPE_CHECKING:
    snakemake: Any

ROUND_DECIMALS: int = 3


@dataclass(frozen=True)
class VoronoiConfig:
    """Voronoi settings from the configuration."""

    enabled: bool
    sample_spacing: int
    min_samples_per_shoreline: int
    max_samples_per_maritime: int
    include_shoreline_vertices: bool
    shoreline_search_radius: int
    uncovered_area_tolerance: float


@dataclass(frozen=True)
class ShorelineLine:
    """A shoreline line and the land shape that generated it."""

    assigned_shape_id: str
    geometry: LineString


def _iter_lines(geom: BaseGeometry) -> Iterator[LineString]:
    """Yield line components from a possibly nested geometry."""
    if geom.is_empty:
        return
    if isinstance(geom, LineString):
        yield geom
    elif isinstance(geom, MultiLineString):
        yield from geom.geoms
    elif isinstance(geom, GeometryCollection):
        for part in geom.geoms:
            yield from _iter_lines(part)


def _merge_lines(lines: list[LineString]) -> list[LineString]:
    """Merge connected line fragments into longer line components."""
    result: list[LineString] = []
    if lines:
        merged = unary_union(lines)
        if isinstance(merged, LineString):
            result = [merged]
        else:
            result = list(_iter_lines(linemerge(merged)))
    return result


def _sample_line_midpoints(
    line: LineString, spacing: float, min_samples: int = 1
) -> list[Point]:
    """Sample evenly spaced midpoint locations along a line."""
    length = line.length
    points: list[Point] = []
    if length != 0:
        n = max(1, min_samples, math.ceil(length / spacing))
        points = [line.interpolate((i + 0.5) * length / n) for i in range(n)]
    return points


def _sample_line_vertices(line: LineString) -> list[Point]:
    """Sample existing line vertices without adding ambiguous open-line endpoints."""
    coords = list(line.coords)
    if len(coords) < 2:
        return []
    if line.is_ring:
        coords = coords[:-1]
    else:
        coords = coords[1:-1]
    return [Point(coord) for coord in coords]


def _collect_shoreline_lines(
    maritime_row, land: gpd.GeoDataFrame, *, shoreline_search_radius: float
) -> list[ShorelineLine]:
    """Find marine-facing exterior shoreline lines for a maritime shape."""
    country_boundary = land.geometry.union_all().boundary
    coastal_band = maritime_row.geometry.buffer(max(0, shoreline_search_radius))
    candidate_land = land.iloc[land.sindex.query(coastal_band, predicate="intersects")]

    shoreline_lines: list[ShorelineLine] = []
    for land_row in candidate_land.itertuples(index=False):
        shoreline = land_row.geometry.boundary.intersection(country_boundary)
        shoreline = shoreline.intersection(coastal_band)
        shoreline_lines.extend(
            ShorelineLine(land_row.shape_id, line)
            for line in _merge_lines(list(_iter_lines(shoreline)))
        )
    return shoreline_lines


def _deduplicate_seed_records(
    seeds: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Keep one seed per coordinate and drop coordinates assigned to multiple shapes."""
    grouped: dict[tuple[float, float], list[dict[str, object]]] = {}
    for seed in seeds:
        point = seed["geometry"]
        if not isinstance(point, Point):
            raise TypeError("Expected shoreline seed geometry to be a Point.")
        key = (round(point.x, ROUND_DECIMALS), round(point.y, ROUND_DECIMALS))
        grouped.setdefault(key, []).append(seed)

    deduplicated = []
    for matches in grouped.values():
        assigned_ids = {match["assigned_shape_id"] for match in matches}
        if len(assigned_ids) == 1:
            deduplicated.append(matches[0])
    return deduplicated


def _sample_lines(
    shoreline_lines: list[ShorelineLine],
    spacing: float,
    min_samples: int,
    include_vertices: bool,
) -> list[dict[str, object]]:
    """Convert shoreline lines into assigned Voronoi seed records."""
    sampled_seeds: list[dict[str, object]] = []
    for shoreline_line in shoreline_lines:
        points = _sample_line_midpoints(shoreline_line.geometry, spacing, min_samples)
        if include_vertices:
            points.extend(_sample_line_vertices(shoreline_line.geometry))

        sampled_seeds.extend(
            {"assigned_shape_id": shoreline_line.assigned_shape_id, "geometry": p}
            for p in points
        )
    return _deduplicate_seed_records(sampled_seeds)


def _make_shoreline_seed_records(
    shoreline_lines: list[ShorelineLine],
    *,
    sample_spacing: float,
    min_samples_per_shoreline: int,
    max_samples_per_maritime: int,
    include_shoreline_vertices: bool,
) -> list[dict[str, object]]:
    """Create bounded shoreline seed records for one maritime split."""
    min_samples_per_shoreline = max(1, min_samples_per_shoreline)
    max_samples_per_maritime = max(1, max_samples_per_maritime)

    seeds = _sample_lines(
        shoreline_lines,
        sample_spacing,
        min_samples_per_shoreline,
        include_shoreline_vertices,
    )
    if len(seeds) > max_samples_per_maritime:
        total_length = sum(line.geometry.length for line in shoreline_lines)
        adaptive_spacing = max(
            sample_spacing, total_length / max(1, max_samples_per_maritime)
        )
        adaptive_min_samples = max(
            1,
            min(
                min_samples_per_shoreline,
                max_samples_per_maritime // max(1, len(shoreline_lines)),
            ),
        )
        seeds = _sample_lines(
            shoreline_lines,
            adaptive_spacing,
            adaptive_min_samples,
            include_vertices=False,
        )
    return seeds


def _split_one_maritime(
    maritime_row, land: gpd.GeoDataFrame, *, crs: CRS, voronoi_config: VoronoiConfig
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Split a single maritime shape to fit the coastline."""
    if land.empty:
        raise ValueError(
            f"No land shapes found for country_id={maritime_row.country_id!r}."
        )

    sample_spacing = voronoi_config.sample_spacing
    shoreline_lines = _collect_shoreline_lines(
        maritime_row,
        land,
        shoreline_search_radius=voronoi_config.shoreline_search_radius,
    )
    seeds = _make_shoreline_seed_records(
        shoreline_lines,
        sample_spacing=sample_spacing,
        min_samples_per_shoreline=voronoi_config.min_samples_per_shoreline,
        max_samples_per_maritime=voronoi_config.max_samples_per_maritime,
        include_shoreline_vertices=voronoi_config.include_shoreline_vertices,
    )
    if not seeds:
        raise ValueError(
            f"No touching shoreline regions found for maritime shape {maritime_row.shape_id!r}."
        )
    seed_gdf = gpd.GeoDataFrame(seeds, geometry="geometry", crs=crs)

    base_row = maritime_row._asdict()
    assigned_ids = seed_gdf["assigned_shape_id"].unique()
    if len(assigned_ids) == 1:
        # Nothing to do
        pieces = gpd.GeoDataFrame([base_row.copy()], geometry="geometry", crs=crs)
        cells = gpd.GeoDataFrame(geometry=[], crs=crs)
    else:
        # Run Voronoi
        maritime_geometry = maritime_row.geometry
        cells = gpd.GeoDataFrame(
            geometry=list(
                voronoi_polygons(
                    MultiPoint(list(seed_gdf.geometry)),
                    extend_to=maritime_geometry.envelope.buffer(sample_spacing * 2),
                ).geoms
            ),
            crs=crs,
        )
        cells = _geo.make_geometries_valid(cells)

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
        cells["geometry"] = cells.geometry.intersection(maritime_geometry)
        cells = _geo.make_geometries_valid(cells)
        cells = cells.loc[cells.geometry.area > 0]
        pieces = (
            cells[["assigned_shape_id", "geometry"]]
            .dissolve(by="assigned_shape_id", as_index=False)
            .rename(columns={"assigned_shape_id": "_assigned_shape_id"})
        )
        pieces = _geo.make_geometries_valid(pieces)
        uncovered = _geo.make_geometry_valid(
            maritime_geometry.difference(pieces.geometry.union_all())
        )
        uncovered_area = 0 if uncovered is None else uncovered.area
        if uncovered_area > voronoi_config.uncovered_area_tolerance:
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

    return pieces, cells


def split_maritime_by_shoreline_voronoi(
    shapes: gpd.GeoDataFrame, *, crs: dict[str, CRS], voronoi_config: VoronoiConfig
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Split EEZ zones to fit shoreline land regions."""
    cells = gpd.GeoDataFrame(geometry=[], crs=shapes.crs)
    if not voronoi_config.enabled:
        return shapes, cells

    shapes_proj = _geo.to_projected_crs(shapes, crs["projected"])
    shapes_proj = _geo.make_geometries_valid(shapes_proj)
    land = shapes_proj.loc[shapes_proj["shape_class"].eq("land")].copy()
    maritime = shapes_proj.loc[shapes_proj["shape_class"].eq("maritime")].copy()
    if maritime.empty:
        raise ValueError("Requested voronoi without maritime shapes.")

    split_results = [
        _split_one_maritime(
            maritime_row,
            land.loc[land["country_id"].eq(maritime_row.country_id)],
            voronoi_config=voronoi_config,
            crs=crs["projected"],
        )
        for maritime_row in maritime.itertuples(index=False)
    ]

    split_maritime = [pieces for pieces, _ in split_results]
    result = gpd.GeoDataFrame(
        pd.concat([land, *split_maritime], ignore_index=True),
        geometry="geometry",
        crs=crs["projected"],
    )

    cell_frames = [i for _, i in split_results if not i.empty]
    if cell_frames:
        cells = gpd.GeoDataFrame(
            pd.concat(cell_frames, ignore_index=True),
            geometry="geometry",
            crs=crs["projected"],
        )
    result = _geo.to_geographic_crs(result, crs["geographic"])
    return result, cells


def build_country(
    land: gpd.GeoDataFrame, maritime: gpd.GeoDataFrame, crs: dict[str, CRS]
) -> gpd.GeoDataFrame:
    """Build country dataset, with maritime regions if necessary."""
    if maritime.empty:
        return _geo.to_geographic_crs(land, crs["geographic"])

    # Reproject
    p_land = _geo.to_projected_crs(land, crs["projected"])
    p_marine = _geo.to_projected_crs(maritime, crs["projected"])
    p_land = _geo.make_geometries_valid(p_land)
    p_marine = _geo.make_geometries_valid(p_marine)
    # Remove contested zones
    p_marine = p_marine[p_marine["contested"].eq(False)].drop(columns="contested")
    # Give priority to EEZs
    p_land.geometry = p_land.geometry.difference(p_marine.geometry.union_all())
    p_land = _geo.make_geometries_valid(p_land)
    combined = gpd.GeoDataFrame(
        pd.concat([p_land, p_marine], ignore_index=True), crs=crs["projected"]
    )
    return _geo.to_geographic_crs(combined, crs["geographic"])


def plot_voronoi_cells(ax: _utils.Axes, cells: gpd.GeoDataFrame, crs: CRS) -> None:
    """Show voronoi tessellation."""
    projected_cells = cells.to_crs(crs)
    projected_cells.boundary.plot(ax=ax, lw=0.2, color="lightgrey", zorder=0)


def main() -> None:
    """Main snakemake process."""
    crs = _geo.check_crs_config(snakemake.params.crs)
    country = snakemake.wildcards.country

    # Load and ensure request matches expectations
    land = _schemas.ShapesSchema.validate(gpd.read_parquet(snakemake.input.land))
    maritime = _schemas.EEZSchema.validate(gpd.read_parquet(snakemake.input.maritime))
    country_ids = set(land["country_id"]) | set(maritime["country_id"])
    if set(country_ids) - {country}:
        raise ValueError(
            f"Country mismatch: expected {country!r}, found {country_ids!r}."
        )

    # Build default country dataset
    shapes = build_country(land, maritime, crs)
    shapes = _schemas.ShapesSchema.validate(shapes)

    # If requested, run Voronoi splitting
    voronoi_config = VoronoiConfig(**snakemake.params.voronoi)
    cells = None
    if not maritime.empty and voronoi_config.enabled:
        shapes, cells = split_maritime_by_shoreline_voronoi(
            shapes, crs=crs, voronoi_config=voronoi_config
        )
        shapes = _schemas.ShapesSchema.validate(shapes)

    # Save and show a pretty plot
    shapes.to_parquet(snakemake.output.country)
    fig, ax = _utils.plot_shapes(shapes, crs["projected"])
    if cells is not None and not cells.empty:
        plot_voronoi_cells(ax, cells, crs["projected"])
    fig.savefig(snakemake.output.plot, dpi=200, bbox_inches="tight")


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
