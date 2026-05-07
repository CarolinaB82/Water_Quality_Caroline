from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    to_date,
    row_number
)
from pyspark.sql.window import Window

from src.pyspark.config.spark_session import get_spark_session


def run_silver_stations():
    spark = get_spark_session("WaterQuality-Silver-Stations")

    try:
        input_path = "data/bronze/pyspark/stations"
        output_path = "data/silver/pyspark/stations"

        df = spark.read.parquet(input_path)

        df = (
            df
            .withColumnRenamed("inseecommune", "code_commune")
            .withColumnRenamed("nomcommune", "nom_commune")
            .withColumnRenamed("quartier", "nom_quartier")
            .withColumnRenamed("cdreseau", "code_reseau")
            .withColumnRenamed("nomreseau", "nom_reseau")
            .withColumnRenamed("debutalim", "debut_alim")
        )

        for c in ["code_commune", "code_reseau"]:
            if c in df.columns:
                df = df.withColumn(c, upper(trim(col(c))))

        for c in ["nom_commune", "nom_quartier", "nom_reseau"]:
            if c in df.columns:
                df = df.withColumn(c, initcap(trim(col(c))))

        if "debut_alim" in df.columns:
            df = df.withColumn("debut_alim", to_date(col("debut_alim")))

        for c in ["code_commune", "code_reseau"]:
            if c in df.columns:
                df = df.filter(col(c).isNotNull())

        df = df.dropDuplicates([
            "code_commune",
            "code_reseau",
            "debut_alim"
        ])

        window_spec = Window.orderBy(
            "code_commune",
            "code_reseau",
            "debut_alim"
        )

        df = df.withColumn(
            "stations_id",
            row_number().over(window_spec)
        )

        cols = ["stations_id"] + [
            c for c in df.columns if c != "stations_id"
        ]

        df = df.select(*cols)

        df.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Silver stations PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_stations()