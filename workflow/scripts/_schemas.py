"""Reusable schemas."""

import geopandas as gpd
from _geo import extract_polygonal_geometry
from pandera import pandas as pa
from pandera.typing.geopandas import GeoSeries
from pandera.typing.pandas import Series
from shapely.geometry import MultiPolygon, Polygon

SUPPORTED_DATASETS = ["gadm", "overture", "marineregions", "nuts", "geoboundaries"]


class ShapesSchema(pa.DataFrameModel):
    """Schema for geographic shapes."""

    class Config:
        coerce = True
        strict = True

    shape_id: Series[str] = pa.Field(unique=True)
    "A unique identifier for this shape."
    country_id: Series[str]
    "Country ISO alpha-3 code."
    shape_class: Series[str] = pa.Field(isin=["land", "maritime"])
    "Identifier of the shape's context."
    geometry: GeoSeries
    "Shape (multi)polygon."
    parent: Series[str] = pa.Field(isin=SUPPORTED_DATASETS)
    "Parent dataset."
    parent_subtype: Series[str]
    "Region disaggregation level in the parent dataset."
    parent_id: Series[str]
    "Unique id in the parent dataset."
    parent_name: Series[str]
    "Human-readable name in the parent dataset."

    @pa.dataframe_parser
    def fix_geometries(cls, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:  # type: ignore[misc]
        """Attempt to correct empty, malformed, or non-polygonal geometries."""
        mask = gdf["geometry"].notna() & ~gdf["geometry"].is_empty
        gdf = gdf.loc[mask].copy()

        invalid = ~gdf.geometry.is_valid
        gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].make_valid()

        gdf["geometry"] = gdf["geometry"].apply(extract_polygonal_geometry)
        return gdf.loc[gdf["geometry"].notna()]

    @pa.check("geometry", element_wise=True)
    def check_geometries(cls, geom) -> bool:
        return (
            isinstance(geom, (Polygon, MultiPolygon))
            and not geom.is_empty
            and geom.is_valid
        )


class EEZSchema(ShapesSchema):
    """Schema for marine shapes."""

    contested: Series[bool]
    """Specifies if the EEZ is contested."""
