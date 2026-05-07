from src.pyspark.config.spark_session import get_spark_session


def run_gold_dim_stations():
    spark = get_spark_session("WaterQuality-Gold-DimStations")

    try:
        input_path = "data/silver/pyspark/stations"
        output_path = "data/gold/pyspark/dim_stations"

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

        print(f"✅ Gold dim_stations PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_dim_stations()