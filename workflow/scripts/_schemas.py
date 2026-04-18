import pandera.pandas as pa
from pandera.typing.geopandas import GeoSeries
from pandera.typing.pandas import Series
from shapely.validation import make_valid


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
    parent: Series[str] = pa.Field(isin=["gadm", "overture", "marineregions", "nuts"])
    "Parent dataset."
    parent_subtype: Series[str]
    "Region disaggregation level in the parent dataset."
    parent_id: Series[str]
    "Unique id in the parent dataset."
    parent_name: Series[str]
    "Human-readable name in the parent dataset."

    @classmethod
    @pa.dataframe_parser
    def fix_geometries(cls, df):
        """Attempt to correct empty or malformed geometries."""
        mask = df["geometry"].apply(lambda g: (g is not None) and (not g.is_empty))
        df = df.loc[mask]
        df["geometry"] = df["geometry"].apply(
            lambda g: g if g.is_valid else make_valid(g)
        )
        return df

    @pa.check("geometry", element_wise=True)
    def check_geometries(cls, geom):
        return (geom is not None) and (not geom.is_empty) and geom.is_valid

