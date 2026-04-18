"""Rules to used to download resource files.

Small transformations might be performed to make the data easier to work with.
"""


rule download_country_overture:
    output:
        path="<resources>/automatic/countries/overture_{country}_{subtype}.parquet",
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
            "<resources>/automatic/countries/raw_gadm_{country}_{subtype}.parquet"
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
        raw="<resources>/automatic/countries/raw_gadm_{country}_{subtype}.parquet",
    output:
        standardised="<resources>/automatic/countries/gadm_{country}_{subtype}.parquet",
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
        path="<resources>/automatic/nuts/nuts_{resolution}_{year}_{level}.parquet",
    log:
        "<logs>/download_nuts_{resolution}_{year}_{level}.log",
    conda:
        "../envs/shape.yaml"
    params:
        epsg=internal["nuts"]["epsg"],
    message:
        "Download '{wildcards.resolution}_{wildcards.year}_{wildcards.level}' from NUTS."
    script:
        "../scripts/download_nuts.py"


rule standardise_country_nuts:
    input:
        raw=lambda wc: f"<resources>/automatic/nuts/nuts_{config["countries"][wc.country]["resolution"]}_{config["countries"][wc.country]["year"]}_{wc.subtype}.parquet",
    output:
        path="<resources>/automatic/countries/nuts_{country}_{subtype}.parquet",
    log:
        "<logs>/{country}/standardise_country_nuts_{subtype}.log",
    conda:
        "../envs/shape.yaml"
    params:
        year=lambda wc: config["countries"][wc.country]["year"],
    message:
        "Standardise '{wildcards.country}_{wildcards.subtype}' NUTS dataset."
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
