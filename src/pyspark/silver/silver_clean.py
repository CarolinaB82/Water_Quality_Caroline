from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WaterQuality").getOrCreate()

df = spark.read.parquet("data/bronze/pyspark/analyses_test")

df_clean = (
    df
    .dropDuplicates()
    .dropna(subset=["cddept", "referenceprel"])
)

df_clean.write.mode("overwrite").parquet("data/silver/pyspark/analyses_clean")

print("✅ Silver PySpark OK")