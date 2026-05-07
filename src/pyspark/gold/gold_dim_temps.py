from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    quarter,
    row_number,
    to_date
)
from pyspark.sql.window import Window

from src.pyspark.config.spark_session import get_spark_session


def run_gold_dim_temps():
    spark = get_spark_session("WaterQuality-Gold-DimTemps")

    try:
        input_path = "data/silver/pyspark/analyses"
        output_path = "data/gold/pyspark/dim_temps"

        df = spark.read.parquet(input_path)

        if "date_prelevement" not in df.columns:
            raise Exception(
                "La colonne date_prelevement est absente du dataset analyses"
            )

        dim_temps = (
            df
            .select(
                to_date(col("date_prelevement")).alias("date")
            )
            .dropna()
            .dropDuplicates(["date"])
            .withColumn("annee", year(col("date")))
            .withColumn("mois", month(col("date")))
            .withColumn("jour", dayofmonth(col("date")))
            .withColumn("trimestre", quarter(col("date")))
        )

        window_spec = Window.orderBy("date")

        dim_temps = dim_temps.withColumn(
            "date_id",
            row_number().over(window_spec)
        )

        dim_temps = dim_temps.select(
            "date_id",
            "date",
            "annee",
            "mois",
            "jour",
            "trimestre"
        )

        dim_temps.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Gold dim_temps PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_dim_temps()