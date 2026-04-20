"""Reusable utility functions."""

import geopandas as gpd
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pyproj import CRS


def check_crs_config(crs: dict[str, int | str]) -> dict[str, CRS]:
    """Check the crs configuration settings."""
    result = {k: CRS.from_user_input(v) for k, v in crs.items()}
    if not result["projected"].is_projected:
        raise ValueError(f"CRS must be projected. Got {crs['projected']!r}.")
    if not result["geographic"].is_geographic:
        raise ValueError(f"CRS must be geographic. Got {crs['geographic']!r}.")
    return result


def plot_shapes(shapes: gpd.GeoDataFrame, crs: str | int | CRS) -> tuple[Figure, Axes]:
    """Generate a nice figure of dataframes that fit the module's schema."""
    gdf = shapes.copy().to_crs(crs)
    fig, ax = plt.subplots(layout="constrained")
    gdf.boundary.plot(ax=ax, color="black", lw=0.5)
    ax = gdf.plot(ax=ax, column="shape_class", legend=False)
    ax.set(xticks=[], yticks=[], xlabel="", ylabel="")
    return fig, ax
