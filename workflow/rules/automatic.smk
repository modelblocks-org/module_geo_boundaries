"""Rules to used to download resource files.

Small transformations might be performed to make the data easier to work with.
"""


rule download_country_overture:
    message:
        "Download '{wildcards.country}_{wildcards.subtype}' dataset from Overture Maps."
    params:
        version=config["overture_release"],
    output:
        path="<resources>/automatic/countries/overture_{country}_{subtype}.parquet",
    log:
        "<logs>/download_country_overture_{country}_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    script:
        "../scripts/download_country_overture.py"


rule download_country_gadm:
    message:
        "Download '{wildcards.country}_{wildcards.subtype}' dataset from GADM."
    output:
        path=temp(
            "<resources>/automatic/countries/raw_gadm_{country}_{subtype}.parquet"
        ),
    log:
        "<logs>/download_country_gadm_{country}_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    script:
        "../scripts/download_country_gadm.py"


rule standardise_country_gadm:
    message:
        "Standardise '{wildcards.country}_{wildcards.subtype}' GADM dataset."
    params:
        country_id=lambda wc: str(wc.country),
        subtype=lambda wc: str(wc.subtype),
    input:
        raw="<resources>/automatic/countries/raw_gadm_{country}_{subtype}.parquet",
    output:
        standardised="<resources>/automatic/countries/gadm_{country}_{subtype}.parquet",
    log:
        "<logs>/standardise_country_gadm_{country}_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    script:
        "../scripts/standardise_country_gadm.py"


rule download_nuts:
    message:
        "Download '{wildcards.resolution}_{wildcards.year}_{wildcards.level}' from NUTS."
    params:
        epsg=internal["nuts"]["epsg"],
    output:
        path="<resources>/automatic/nuts/nuts_{resolution}_{year}_{level}.parquet",
    log:
        "<logs>/download_nuts_{resolution}_{year}_{level}.log",
    conda:
        "../envs/shape.yaml"
    script:
        "../scripts/download_nuts.py"


rule standardise_country_nuts:
    message:
        "Standardise '{wildcards.country}_{wildcards.subtype}' NUTS dataset."
    params:
        year=lambda wc: config["countries"][wc.country]["year"],
    input:
        raw=lambda wc: f"<resources>/automatic/nuts/nuts_{config["countries"][wc.country]["resolution"]}_{config["countries"][wc.country]["year"]}_{wc.subtype}.parquet",
    output:
        path="<resources>/automatic/countries/nuts_{country}_{subtype}.parquet",
    log:
        "<logs>/standardise_country_nuts_{country}_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    script:
        "../scripts/standardise_country_nuts.py"


rule download_marine_eez_area:
    message:
        "Download and standardise '{wildcards.country}' EEZ dataset."
    output:
        path="<resources>/automatic/eez/{country}.parquet",
        plot="<resources>/automatic/eez/{country}.png",
    log:
        "<logs>/{country}/download_marine_eez_area.log",
    conda:
        "../envs/shape.yaml"
    script:
        "../scripts/download_marine_eez_area.py"
