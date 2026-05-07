from src.pyspark.config.spark_session import get_spark_session


def run_gold_dim_parametres():
    spark = get_spark_session("WaterQuality-Gold-DimParametres")

    try:
        input_path = "data/pyspark/silver/parametres"
        output_path = "data/pyspark/gold/dim_parametres"

        df = spark.read.parquet(input_path)

        columns_to_drop = [
            "source_api",
            "ingestion_timestamp",
            "page_api"
        ]

        existing_columns = [
            col for col in columns_to_drop if col in df.columns
        ]

        if existing_columns:
            df = df.drop(*existing_columns)

        df = df.dropDuplicates()

        df.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Gold dim_parametres PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_dim_parametres()