"""Download division area data from the Overture Maps foundation."""

import re
import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import boto3
import duckdb
import geopandas as gpd
import pycountry
from _schemas import ShapesSchema
from botocore import UNSIGNED
from botocore.config import Config

if TYPE_CHECKING:
    snakemake: Any


S3_REGION = "us-west-2"
S3_BUCKET = f"overturemaps-{S3_REGION}"
S3_RELEASE_PREFIX = "release/"
S3_GLOB = "s3://{bucket}/release/{version}/theme=divisions/type=division_area/*"

RE_RELEASE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\.(\d+)$")


def _parse_release(release: str) -> tuple[int, int, int, int]:
    """Splits release numbers into tuples: yyyy.mm.dd.x -> (yyyy, mm, dd, x)."""
    m = RE_RELEASE.match(release)
    if not m:
        # Releases not matching the yyyy.mm.dd.x formatting have the lowest priority
        release_split = (0, 0, 0, 0)
    else:
        y, mo, d, n = m.groups()
        release_split = (int(y), int(mo), int(d), int(n))
    return release_split


def _get_overture_releases(s3) -> Iterable[str]:
    """Yield immediate children under 'release/' as version names (e.g., 'yyyy-mm-dd.x')."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=S3_RELEASE_PREFIX, Delimiter="/"
    ):
        for pref in page.get("CommonPrefixes", []):
            # e.g. 'release/2025-09-24.0/' -> '2025-09-24.0'
            full = pref["Prefix"].rstrip("/")
            name = full.split("/", 1)[1]
            yield name


def _check_release(s3, version: str) -> bool:
    """Check if a release version exists in the S3 bucket."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{S3_RELEASE_PREFIX}{version}/", MaxKeys=1
    )
    return resp.get("KeyCount", 0) > 0


def _resolve_overture_glob(version: str) -> str:
    """Return a valid Overture glob for the divisions dataset.

    Args:
        version (str): version to resolve. Can be Calver ('yyyy.mm.dd.x') or 'latest'.

    Returns:
        str: valid Overture S3 glob.
    """
    s3 = boto3.client(
        "s3", region_name=S3_REGION, config=Config(signature_version=UNSIGNED)
    )
    releases = list(_get_overture_releases(s3))
    if not releases:
        raise RuntimeError(
            "Could not fetch Overture releases from the public S3 bucket."
        )
    if version == "latest":
        version = max(releases, key=_parse_release)
    else:
        if not _check_release(s3, version):
            raise ValueError(
                f"Requested version {version} is not in release list {releases}."
            )

    return S3_GLOB.format(bucket=S3_BUCKET, version=version)


def download_country_overture(country: str, subtype: str, version: str, path: str):
    """Download country division areas from Overture maps.

    Uses duckdb for remote interfacing and 'larger than memory' file generation.
    """
    # Prepare variables for the request
    country_a2 = pycountry.countries.get(alpha_3=country).alpha_2
    overture_glob = _resolve_overture_glob(version)

    # Setup SQL connection to the remote dataset
    connection = duckdb.connect()
    for extension in ["spatial", "httpfs"]:
        connection.load_extension(extension)
    connection.sql("SET s3_region='us-west-2'")

    # Request country dataset with added metadata
    connection.sql(
        f"""
        COPY (
            SELECT
                '{country}' || '_' || 'overture' || '_' || id AS shape_id,
                '{country}' AS country_id,
                class AS shape_class,
                geometry,
                'overture' AS parent,
                subtype AS parent_subtype,
                id AS parent_id,
                names.primary AS parent_name
            FROM
                read_parquet(
                    '{overture_glob}',
                    filename=true,
                    hive_partitioning=true
                )
            WHERE
                country == '{country_a2}'
                AND subtype == '{subtype}'
                AND class == 'land'
        )
        TO '{path}'
        WITH (
            FORMAT parquet,
            COMPRESSION zstd
        );
        """
    )


def validate_country_overture(
    path: str, country: str, subtype: str, version: str
) -> None:
    """Run quick checks against our schema."""
    gdf = gpd.read_parquet(path)
    if gdf.empty:
        raise ValueError(
            f"Invalid request for '{country}-{subtype}-{version}'. "
            "Please evaluate your request at https://overturemaps.org/."
        )
    ShapesSchema.validate(gdf)


def main() -> None:
    """Main snakemake process."""
    download_country_overture(
        country=snakemake.wildcards.country,
        subtype=snakemake.wildcards.subtype,
        version=snakemake.params.version,
        path=snakemake.output.path,
    )
    validate_country_overture(
        country=snakemake.wildcards.country,
        subtype=snakemake.wildcards.subtype,
        version=snakemake.params.version,
        path=snakemake.output.path,
    )


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
