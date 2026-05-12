"""Reusable utility functions."""

import time
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import requests
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pyproj import CRS

RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}
RETRY_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


@dataclass
class DownloadTimeouts:
    """Generic class for handling timeouts across data sources."""

    max_retries: int
    connect_seconds: int
    read_seconds: int
    initial_retry_seconds: int

    @property
    def request_timeout(self) -> tuple[int, int]:
        return (self.connect_seconds, self.read_seconds)


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


def retry_wait_seconds(
    retry_number: int, initial_wait: float, *, max_wait: float = 60
) -> None:
    """Return exponential backoff wait time for a retry number starting at 0."""
    timeout = min(initial_wait * 2 ** (retry_number), max_wait)
    time.sleep(timeout)


def download_file(url: str, path: Path, timeouts: DownloadTimeouts) -> None:
    """Download URL content to path, retrying transient failures."""
    max_retries = timeouts.max_retries
    for attempt in range(max_retries + 1):
        try:
            with requests.get(url, timeout=timeouts.request_timeout) as response:
                if response.status_code in RETRY_STATUS_CODES and attempt < max_retries:
                    retry_wait_seconds(attempt, timeouts.initial_retry_seconds)
                    continue

                response.raise_for_status()
                path.write_bytes(response.content)
                return
        except RETRY_EXCEPTIONS as exc:
            if attempt == max_retries:
                raise RuntimeError(f"Failed download for {url!r}.") from exc
            retry_wait_seconds(attempt, timeouts.initial_retry_seconds)

    raise RuntimeError(
        f"Maximum retries {max_retries} reached without a valid response"
    )
