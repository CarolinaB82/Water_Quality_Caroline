import glob

from pyspark.sql.functions import input_file_name, regexp_extract

from src.pyspark.config.spark_session import get_spark_session


def run_bronze_parametres():
    spark = get_spark_session("WaterQuality-Bronze-Parametres")

    try:
        files = glob.glob("data/raw/DIS_RESULT_*.txt")

        if not files:
            raise FileNotFoundError(
                "Aucun fichier trouvé avec le pattern data/raw/DIS_RESULT_*.txt"
            )

        df_final = (
            spark.read
            .option("header", True)
            .option("delimiter", ",")
            .option("inferSchema", False)
            .csv(files)
            .withColumn("source_file", input_file_name())
            .withColumn(
                "annee_fichier",
                regexp_extract(input_file_name(), r"DIS_RESULT_(\d{4})\.txt", 1)
            )
        )

        output_path = "data/pyspark/bronze/parametres"

        df_final.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Bronze parametres PySpark OK : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_parametres()