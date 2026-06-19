"""Combine country shapes and marine regions into one harmonized dataset."""

import sys
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import _geo
import _schemas
import _utils
import geopandas as gpd
import pandas as pd
import shapely
from pyproj import CRS

if TYPE_CHECKING:
    snakemake: Any


def remove_overlaps(gdf: gpd.GeoDataFrame, crs: dict[str, CRS]) -> gpd.GeoDataFrame:
    """Remove overlaps deterministically by keeping earlier rows and clipping later rows.

    The first row wins. Any area shared with an earlier kept geometry is removed
    from the later geometry.
    """
    projected = _geo.to_projected_crs(gdf, crs["projected"])
    projected = _geo.make_geometries_valid(projected)

    left_idx, right_idx = projected.sindex.query(
        projected.geometry, predicate="intersects"
    )

    overlaps_by_right: dict[int, list[int]] = defaultdict(list)
    for left, right in zip(left_idx, right_idx, strict=True):
        if left >= right:
            continue
        overlaps_by_right[right].append(left)

    geoms = projected.geometry.to_list()
    for right in range(len(geoms)):
        geom = geoms[right]
        if geom is None or geom.is_empty:
            continue

        cutters = [
            geoms[left]
            for left in overlaps_by_right.get(right, [])
            if geoms[left] is not None
            and not geoms[left].is_empty
            and geom.intersects(geoms[left])
        ]
        if not cutters:
            continue

        geom = geom.difference(shapely.unary_union(cutters))
        geoms[right] = _geo.make_geometry_valid(geom)

    projected["geometry"] = geoms
    projected = projected.loc[projected.geometry.notna()]
    result = _geo.to_geographic_crs(projected, crs["geographic"])

    return result


def main() -> None:
    """Main snakemake process."""
    crs = _geo.check_crs_config(snakemake.params.crs)

    # Load and ensure inputs are healthy
    country_list = [gpd.read_parquet(i) for i in snakemake.input.countries]
    crs_mismatch = [not crs["geographic"].equals(i.crs) for i in country_list]
    if any(crs_mismatch):
        raise ValueError(f"Received datasets with invalid CRS: {sum(crs_mismatch)!r} .")
    combined = gpd.GeoDataFrame(
        pd.concat(country_list, ignore_index=True),
        crs=crs["geographic"],
        geometry="geometry",
    )

    combined = remove_overlaps(combined, crs)
    combined = _schemas.ShapesSchema.validate(combined)
    combined.to_parquet(snakemake.output.combined)

    fig, _ = _utils.plot_shapes(combined, crs["projected"])
    fig.savefig(snakemake.output.plot, dpi=200, bbox_inches="tight")


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
