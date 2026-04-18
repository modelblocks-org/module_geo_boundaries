"""Rules to used to download resource files.

Small transformations might be performed to make the data easier to work with.
"""


rule download_country_overture:
    output:
        path="<resources>/automatic/land/overture_{country}_{subtype}.parquet",
    log:
        "<logs>/{country}/download_country_overture_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    params:
        version=config["overture_release"],
    message:
        "Download '{wildcards.country}_{wildcards.subtype}' dataset from Overture Maps."
    script:
        "../scripts/download_country_overture.py"


rule download_country_gadm:
    output:
        path=temp(
            "<resources>/automatic/land/raw_gadm_{country}_{subtype}.parquet"
        ),
    log:
        "<logs>/{country}/download_country_gadm_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    message:
        "Download '{wildcards.country}_{wildcards.subtype}' dataset from GADM."
    script:
        "../scripts/download_country_gadm.py"


rule standardise_country_gadm:
    input:
        raw=rules.download_country_gadm.output.path,
    output:
        standardised="<resources>/automatic/land/gadm_{country}_{subtype}.parquet",
    log:
        "<logs>/{country}/standardise_country_gadm_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    params:
        country_id=lambda wc: str(wc.country),
        subtype=lambda wc: str(wc.subtype),
    message:
        "Standardise '{wildcards.country}_{wildcards.subtype}' GADM dataset."
    script:
        "../scripts/standardise_country_gadm.py"


rule download_nuts:
    output:
        path="<resources>/automatic/nuts/nuts_{subtype}_{resolution}_{year}.parquet",
    log:
        "<logs>/download_nuts_{subtype}_{resolution}_{year}.log",
    conda:
        "../envs/shape.yaml"
    params:
        epsg=internal["nuts"]["epsg"],
    message:
        "Download '{wildcards.subtype}_{wildcards.resolution}_{wildcards.year}' from NUTS."
    script:
        "../scripts/download_nuts.py"


rule standardise_country_nuts:
    input:
        raw=rules.download_nuts.output.path,
    output:
        path="<resources>/automatic/land/nuts_{country}_{subtype}_{year}_{resolution}.parquet",
    log:
        "<logs>/{country}/standardise_country_nuts_{subtype}_{year}_{resolution}.log",
    conda:
        "../envs/shape.yaml"
    message:
        "Standardise '{wildcards.country}' NUTS dataset for '{wildcards.subtype}_{wildcards.resolution}_{wildcards.year}'."
    script:
        "../scripts/standardise_country_nuts.py"


rule download_marine_eez_area:
    output:
        path="<resources>/automatic/eez/{country}.parquet",
        plot="<resources>/automatic/eez/{country}.png",
    log:
        "<logs>/{country}/download_marine_eez_area.log",
    conda:
        "../envs/shape.yaml"
    params:
        extra_eez=lambda wc: config["countries"][wc.country].get("extra_eez", []),
    message:
        "Download and standardise '{wildcards.country}' EEZ dataset."
    script:
        "../scripts/download_marine_eez_area.py"
