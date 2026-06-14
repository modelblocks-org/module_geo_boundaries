"""Rules used to construct the final dataset."""


rule build_eez:
    input:
        country="<resources>/automatic/eez/single/{country}.parquet",
        extra=lambda wc: [
            f"<resources>/automatic/eez/single/{mrgid}.parquet"
            for mrgid in get_extra_eez_from_key(wc.eez_key)
        ],
    output:
        path="<resources>/automatic/eez/combined/{country}_{eez_key}.parquet",
    log:
        "<logs>/eez/build/{country}_{eez_key}.log",
    conda:
        "../envs/shape.yaml"
    message:
        "{wildcards.country}: build EEZ dataset {wildcards.eez_key}."
    script:
        "../scripts/build_eez.py"


rule build_country:
    input:
        land=lambda wc: (
            f"<resources>/automatic/{get_country_file(wc.scenario, wc.country)}.parquet"
        ),
        maritime=lambda wc: (
            f"<resources>/automatic/eez/{get_eez_file(wc.scenario, wc.country)}.parquet"
        ),
    output:
        country="<resources>/automatic/scenarios/{scenario}/{country}.parquet",
        plot=report(
            "<resources>/automatic/scenarios/{scenario}/{country}.png",
            caption="../report/build_country.rst",
            category="Module Geo-Boundaries",
            subcategory="Country area",
        ),
    log:
        "<logs>/scenarios/{scenario}/build_country/{country}.log",
    conda:
        "../envs/shape.yaml"
    params:
        crs=lambda wc: get_crs_config(wc.scenario),
        voronoi=lambda wc: get_voronoi_eez_config(wc.scenario),
    message:
        "{wildcards.scenario}-{wildcards.country}: building combined single-country dataset."
    script:
        "../scripts/build_country.py"


rule build_combined_area:
    input:
        countries=lambda wc: [
            f"<resources>/automatic/scenarios/{wc.scenario}/{country}.parquet"
            for country in config["scenarios"][wc.scenario]["countries"]
        ],
    output:
        combined="<shapes>",
        plot=report(
            "<results>/{scenario}/shapes.png",
            caption="../report/build_combined_area.rst",
            category="Module Geo-Boundaries",
            subcategory="Combined area",
        ),
    log:
        "<logs>/scenarios/{scenario}/build_combined_area.log",
    conda:
        "../envs/shape.yaml"
    params:
        crs=lambda wc: get_crs_config(wc.scenario),
        countries=lambda wc: sorted(
            [i for i in config["scenarios"][wc.scenario]["countries"]]
        ),
        sources=lambda wc: sorted(
            set(
                [
                    i["source"]
                    for i in config["scenarios"][wc.scenario]["countries"].values()
                ]
            )
        ),
    message:
        "{wildcards.scenario}: building combined dataset with all countries."
    script:
        "../scripts/build_combined_area.py"
