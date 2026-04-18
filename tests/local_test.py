"""A collection of heavier tests to run locally.

IMPORTANT: it's recommended to avoid running all tests at the same time!
You'll likely run out of memory.

Instead, run a relevant case:
pytest tests/local_test.py::test_config_example["europe_example"]
"""

import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def module_path():
    """Parent directory of the project."""
    return Path(__file__).parent.parent


@pytest.mark.parametrize("scenario", ["config", "china_example", "europe_example", "usa_example"])
def test_config_example(module_path, scenario):
    """Example files should result in a successful run."""
    result_file = "results/shapes.parquet"
    config_file = Path(module_path / f"config/{scenario}.yaml")
    smk_command = f"snakemake --cores 4 --replace-workflow-config --configfile={config_file} {result_file}"
    subprocess.run(smk_command + " --forceall", shell=True, check=True, cwd=module_path)
    subprocess.run(
        smk_command + " --report results/report.html",
        shell=True,
        check=True,
        cwd=module_path,
    )
    subprocess.run(
        smk_command + " --rulegraph | dot -Tpng > results/rulegraph.png",
        shell=True,
        check=True,
        cwd=module_path,
    )
    assert Path(module_path / result_file).exists()
