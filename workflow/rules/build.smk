"""Rules used to construct the final dataset."""


rule build_combined_area:
    input:
        countries=[
            f"<resources>/automatic/countries/{get_country_filename(country)}.parquet"
            for country in config["countries"]
        ],
        marine=[
            f"<resources>/automatic/eez/{country}.parquet"
            for country in config["countries"]
        ],
    output:
        combined="<shapes>",
        plot=report(
            "<results>/shapes.png",
            caption="../report/results.rst",
            category="Combined area",
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
