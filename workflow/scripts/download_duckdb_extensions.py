"""Download DuckDB extensions needed by datasources using it."""

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    snakemake: Any


def main() -> None:
    """Install DuckDB extensions."""
    connection = duckdb.connect()
    installed_extensions = []
    for extension in ["spatial", "httpfs"]:
        connection.install_extension(extension)
        installed_extensions.append(extension)

    output_path = Path(snakemake.output.path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(installed_extensions) + "\n")


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
