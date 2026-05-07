import glob

from pyspark.sql.functions import input_file_name, regexp_extract

from src.pyspark.config.spark_session import get_spark_session


def run_bronze_analyses():
    spark = get_spark_session("WaterQuality-Bronze-Analyses")

    try:
        files = glob.glob("data/raw/DIS_PLV_*.txt")

        if not files:
            raise FileNotFoundError(
                "Aucun fichier trouvé avec le pattern data/raw/DIS_PLV_*.txt"
            )

        df_final = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .option("inferSchema", False)
    .csv(files)
    .withColumn("source_file", input_file_name())
    .withColumn(
        "annee_fichier",
        regexp_extract(input_file_name(), r"DIS_PLV_(\d{4})\.txt", 1)
    )
)

        df_final.write \
            .mode("overwrite") \
            .parquet("data/bronze/pyspark/analyses")

        print("✅ Bronze PySpark Analyses OK")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_analyses()