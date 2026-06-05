"""Utility functions for snakemake rule handling."""


def get_country_file(country: str):
    """Build unique file names to avoid overwriting source files."""
    source = config["countries"][country]["source"]
    subtype = config["countries"][country]["subtype"]

    filename = f"{source}/harmonise/{country}_{subtype}"
    if source == "nuts":
        resolution = config["countries"][country]["resolution"]
        year = config["countries"][country]["year"]
        filename += f"_{year}_{resolution}"
    elif source == "geoboundaries":
        release = config["countries"][country]["release_type"]
        filename += f"_{release}"

    return filename
