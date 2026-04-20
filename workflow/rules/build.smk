"""Rules used to construct the final dataset."""

rule build_country:
    input:
        land=lambda wc: f"<resources>/automatic/land/{get_country_filename(wc.country)}.parquet",
        maritime="<resources>/automatic/eez/{country}.parquet",
    output:
        country="<resources>/automatic/country/{country}.parquet",
        plot=report(
            "<resources>/automatic/country/{country}.png",
            caption="../report/build_country.rst",
            category="Module Geo-Boundaries",
            subcategory="Combined countries"
        ),
    log:
        "<logs>/{country}/build_country.log",
    conda:
        "../envs/shape.yaml"
    params:
        crs=config["crs"],
        voronoi=internal["voronoi_eez"] | config.get("voronoi_eez", {})
    message:
        "{wildcards.country}: build combined land and marine polygons."
    script:
        "../scripts/build_country.py"


rule build_combined_area:
    input:
        countries=[
            f"<resources>/automatic/country/{country}.parquet"
            for country in config["countries"]
        ],
    output:
        combined="<shapes>",
        plot=report(
            "<results>/shapes.png",
            caption="../report/build_combined_area.rst",
            category="Module Geo-Boundaries",
            subcategory="Combined area",
        ),
    log:
        "<logs>/build_combined_area.log",
    conda:
        "../envs/shape.yaml"
    params:
        crs=config["crs"],
    message:
        "Combine land and marine polygons."
    script:
        "../scripts/build_combined_area.py"
