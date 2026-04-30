"""Utility functions for snakemake rule handling."""


def get_country_filename(country: str):
    """Build unique file names to avoid overwriting source files."""
    source = config["countries"][country]["source"]
    subtype = config["countries"][country]["subtype"]

    filename = f"{source}_{country}_{subtype}"
    if source == "nuts":
        resolution = config["countries"][country]["resolution"]
        year = config["countries"][country]["year"]

        filename += f"_{year}_{resolution}"
    return filename
