"""Rules to used to download resource files."""


rule download_duckdb_extensions:
    output:
        path="<resources>/automatic/overture/duckdb_extensions.txt",
    log:
        "<logs>/overture/download_duckdb_extensions.log",
    localrule: True
    conda:
        "../envs/shape.yaml"
    threads: 1
    message:
        "Downloading DuckDB extensions."
    script:
        "../scripts/download_duckdb_extensions.py"


rule download_geoboundaries:
    output:
        path="<resources>/automatic/geoboundaries/download/{country}_{subtype}_{release_type}.parquet",
    log:
        "<logs>/geoboundaries/download/{country}_{subtype}_{release_type}.log",
    localrule: True
    conda:
        "../envs/shape.yaml"
    params:
        timeouts=internal["timeouts"],
    message:
        "Downloading '{wildcards.country}_{wildcards.subtype}_{wildcards.release_type}' dataset from geoBoundaries."
    script:
        "../scripts/download_geoboundaries.py"


rule download_gadm:
    output:
        path="<resources>/automatic/gadm/download/{country}_{subtype}.parquet",
    log:
        "<logs>/gadm/download/{country}_{subtype}.log",
    localrule: True
    conda:
        "../envs/shape.yaml"
    params:
        timeouts=internal["timeouts"],
    message:
        "Download '{wildcards.country}_{wildcards.subtype}' dataset from GADM."
    script:
        "../scripts/download_gadm.py"


rule download_nuts:
    output:
        path="<resources>/automatic/nuts/download/{subtype}_{resolution}_{year}.parquet",
    log:
        "<logs>/nuts/download/{subtype}_{resolution}_{year}.log",
    localrule: True
    conda:
        "../envs/shape.yaml"
    params:
        timeouts=internal["timeouts"],
    message:
        "Download '{wildcards.subtype}_{wildcards.resolution}_{wildcards.year}' from NUTS."
    script:
        "../scripts/download_nuts.py"
