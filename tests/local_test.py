"""A collection of heavier tests to run locally.

IMPORTANT: it's recommended to avoid running all tests at the same time!
You'll likely run out of memory.

Instead, run a relevant case:
pytest tests/local_test.py::test_config_example
"""

import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "scenario", ["mixed_example", "china_national", "USA_states", "europe_regions"]
)
def test_scenario(module_path: Path, scenario: str):
    """Example files should result in a successful run."""
    result_file = f"results/{scenario}/shapes.parquet"
    smk_command = f"snakemake --cores 4 {result_file}"
    subprocess.run(smk_command + " --forceall", shell=True, check=True, cwd=module_path)
    subprocess.run(
        smk_command + f" --report results/{scenario}/report.html",
        shell=True,
        check=True,
        cwd=module_path,
    )
    subprocess.run(
        smk_command + f" --rulegraph | dot -Tpng > results/{scenario}/rulegraph.png",
        shell=True,
        check=True,
        cwd=module_path,
    )
    assert Path(module_path / result_file).exists()
