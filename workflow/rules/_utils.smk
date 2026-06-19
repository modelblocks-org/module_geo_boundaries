"""Utility functions for snakemake rule handling."""


def get_country_file(scenario: str, country: str):
    """Build unique file names to avoid overwriting source files.

    Scenarios do not affect the downloaded source files.
    """
    country_settings: dict = config["scenarios"][scenario]["countries"][country]

    source = country_settings["source"]
    subtype = country_settings["subtype"]

    filename = f"{source}/harmonise/{country}_{subtype}"
    if source == "nuts":
        resolution = country_settings["resolution"]
        year = country_settings["year"]
        filename += f"_{year}_{resolution}"
    elif source == "geoboundaries":
        release = country_settings["release_type"]
        filename += f"_{release}"

    return filename


def get_crs_config(scenario: str) -> dict:
    """Get CRS configuration in order of priority."""
    return config["crs"] | config["scenarios"][scenario].get("crs", {})


def get_voronoi_eez_config(scenario: str) -> dict:
    """Get Voronoi configuration in order of priority."""
    scenario_override = config["scenarios"][scenario].get("voronoi_eez", {})
    user_overrides = config.get("voronoi_eez", {}) | scenario_override
    return internal["voronoi_eez"] | user_overrides


def get_gdal_config() -> dict:
    """Get GDAL configuration with user overrides."""
    gdal = internal["gdal"] | config.get("gdal", {})
    return gdal


def get_eez_file(scenario: str, country: str) -> str:
    """Build an EEZ filename reusable by scenarios with matching EEZ requests."""
    extra_eez = config["scenarios"][scenario]["countries"][country].get("extra_eez", [])
    if not isinstance(extra_eez, list):
        extra_eez = [extra_eez]

    extra_eez = sorted([int(i) for i in extra_eez])
    if extra_eez:
        file_path = f"combined/{country}_{'_'.join([str(i) for i in extra_eez])}"
    else:
        file_path = f"single/{country}"
    return file_path


def get_extra_eez_from_key(eez_key: str) -> list[int]:
    """Recover additional EEZ identifiers from an EEZ output wildcard."""
    return [int(i) for i in eez_key.split("_")]
