"""Reusable geometry helpers."""

from collections.abc import Iterator
from itertools import pairwise
from warnings import warn

import antimeridian
import geopandas as gpd
from pyproj import CRS
from rasterio import warp
from shapely import MultiPolygon, Polygon
from shapely.geometry import GeometryCollection, mapping, shape
from shapely.geometry.base import BaseGeometry

CRS_MARINE_REGIONS = "EPSG:4326"


def check_crs_config(crs: dict[str, int | str]) -> dict[str, CRS]:
    """Check the crs configuration settings."""
    result = {k: CRS.from_user_input(v) for k, v in crs.items()}
    if not result["projected"].is_projected:
        raise ValueError(f"CRS must be projected. Got {crs['projected']!r}.")
    if not result["geographic"].is_geographic:
        raise ValueError(f"CRS must be geographic. Got {crs['geographic']!r}.")
    return result


def _iter_polygons(geom: BaseGeometry | None) -> Iterator[Polygon]:
    """Yield polygon parts from possibly mixed geometry."""
    if geom is None or geom.is_empty:
        return

    if isinstance(geom, Polygon):
        yield geom
    elif isinstance(geom, (MultiPolygon, GeometryCollection)):
        for part in geom.geoms:
            yield from _iter_polygons(part)


def extract_polygonal_geometry(
    geom: BaseGeometry | None,
) -> Polygon | MultiPolygon | None:
    """Return only Polygon/MultiPolygon components from a geometry."""
    polygons = list(_iter_polygons(geom))
    result = None
    if polygons:
        result = polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
    return result


def _rasterio_to_crs(gdf: gpd.GeoDataFrame, to_crs: CRS) -> gpd.GeoDataFrame:
    """CRS conversion using rasterio's more powerful toolset.

    Compared to geopandas this should adequately handle antimeridian "splitting".
    Order: geopandas -> GeoJSON-like dict -> rasterio -> geopandas
    """
    input_geoms = [mapping(geom) for geom in gdf.geometry]
    transformed = warp.transform_geom(
        src_crs=gdf.crs, dst_crs=to_crs, geom=input_geoms, antimeridian_cutting=True
    )
    output_geoms = [shape(geom) for geom in transformed]
    return gdf.set_geometry(gpd.GeoSeries(output_geoms, index=gdf.index, crs=to_crs))


def to_projected_crs(gdf: gpd.GeoDataFrame, crs: int | str | CRS) -> gpd.GeoDataFrame:
    """CRS conversion using rasterio's more powerful toolset.

    Compared to geopandas this adequately handles handles antimeridian "splitting".
    """
    to_crs = CRS.from_user_input(crs)
    if not to_crs.is_projected:
        raise ValueError(f"This function only converts to geographic CRS. Got {crs!r}.")
    return _rasterio_to_crs(gdf, to_crs)


def _crosses_antimeridian(poly: Polygon) -> bool:
    rings = [poly.exterior, *poly.interiors]

    crosses = any(
        180 < abs(x2 - x1) < 360
        for ring in rings
        for (x1, _), (x2, _) in pairwise(ring.coords)
    )

    return crosses


def _fix_antimeridian_geometry(geom: BaseGeometry) -> BaseGeometry:
    fixed_geom = geom

    if geom.geom_type == "Polygon":
        if _crosses_antimeridian(geom):
            fixed_geom = antimeridian.fix_polygon(geom)

    elif geom.geom_type == "MultiPolygon":
        parts = []

        for poly in geom.geoms:
            fixed_poly = poly

            if _crosses_antimeridian(poly):
                fixed_poly = antimeridian.fix_polygon(poly)

            if fixed_poly.geom_type == "MultiPolygon":
                parts.extend(fixed_poly.geoms)
            else:
                parts.append(fixed_poly)

        fixed_geom = MultiPolygon(parts)

    return fixed_geom


def to_geographic_crs(gdf: gpd.GeoDataFrame, crs: int | str | CRS) -> gpd.GeoDataFrame:
    """Convert to a geographic CRS fixing antimeridian-crossings."""
    target_crs = CRS.from_user_input(crs)
    if not target_crs.is_geographic:
        raise ValueError(f"This function only converts to geographic CRS. Got {crs!r}.")

    # Map to 4326 to ensure we match antimeridian algorithm requirements
    fixed = _rasterio_to_crs(gdf, CRS.from_user_input("EPSG:4326"))
    fixed.geometry = fixed.geometry.map(_fix_antimeridian_geometry)

    if not fixed.crs.equals(target_crs):
        warn(
            f"Target CRS {crs!r} does not match {fixed.crs!r}. "
            "This might behave oddly near the antimeridian."
        )
        fixed = fixed.to_crs(target_crs)

    return fixed
