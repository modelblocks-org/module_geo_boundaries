"""Rules to used to harmonise country files."""


rule harmonise_geoboundaries:
    input:
        raw=rules.download_geoboundaries.output.path,
    output:
        path="<resources>/automatic/geoboundaries/harmonise/{country}_{subtype}_{release_type}.parquet",
    log:
        "<logs>/geoboundaries/harmonise/{country}_{subtype}_{release_type}.log",
    conda:
        "../envs/shape.yaml"
    message:
        "Harmonising '{wildcards.country}_{wildcards.subtype}_{wildcards.release_type}' dataset from geoBoundaries."
    script:
        "../scripts/harmonise_geoboundaries.py"


rule download_harmonised_overture:
    input:
        duckdb_extensions=rules.download_duckdb_extensions.output.path,
    output:
        path="<resources>/automatic/overture/harmonise/{country}_{subtype}.parquet",
    log:
        "<logs>/overture/download_and_harmonise/{country}_{subtype}.log",
    localrule: True
    conda:
        "../envs/shape.yaml"
    params:
        version=config.get("overture_release", internal["overture_release"]),
    message:
        "Downloading harmonised '{wildcards.country}_{wildcards.subtype}' dataset from Overture Maps."
    script:
        "../scripts/download_harmonised_overture.py"


rule harmonise_gadm:
    input:
        raw=rules.download_gadm.output.path,
    output:
        standardised="<resources>/automatic/gadm/harmonise/{country}_{subtype}.parquet",
    log:
        "<logs>/gadm/harmonise/{country}_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    message:
        "Harmonising '{wildcards.country}_{wildcards.subtype}' GADM dataset."
    script:
        "../scripts/harmonise_gadm.py"


rule harmonise_nuts:
    input:
        raw=rules.download_nuts.output.path,
    output:
        path="<resources>/automatic/nuts/harmonise/{country}_{subtype}_{year}_{resolution}.parquet",
    log:
        "<logs>/nuts/harmonise/{country}_{subtype}_{year}_{resolution}.log",
    conda:
        "../envs/shape.yaml"
    message:
        "Harmonising '{wildcards.country}' NUTS dataset for '{wildcards.subtype}_{wildcards.resolution}_{wildcards.year}'."
    script:
        "../scripts/harmonise_nuts.py"


rule download_harmonised_eez:
    output:
        path="<resources>/automatic/eez/single/{eez}.parquet",
        plot=report(
            "<resources>/automatic/eez/single/{eez}.png",
            caption="../report/download_harmonised_eez.rst",
            category="Module Geo-Boundaries",
            subcategory="EEZ area",
        ),
    log:
        "<logs>/eez/download_harmonised/{eez}.log",
    localrule: True
    conda:
        "../envs/shape.yaml"
    params:
        timeouts=internal["timeouts"],
    message:
        "Download and harmonise EEZ dataset '{wildcards.eez}'."
    script:
        "../scripts/download_harmonised_eez.py"
