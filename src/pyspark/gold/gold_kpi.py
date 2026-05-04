from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

spark = SparkSession.builder.appName("WaterQuality").getOrCreate()

df = spark.read.parquet("data/silver/pyspark/analyses_clean")

df = df.withColumn(
    "is_conforme",
    when(
        (col("plvconformitebacterio") == "C") &
        (col("plvconformitechimique") == "C"),
        1
    ).otherwise(0)
)

df_grouped = df.groupBy(
    "nomcommuneprinc",
    "cddept",
    "annee_fichier"
).agg(
    count("*").alias("nb_prelevements"),
    count(when(col("is_conforme") == 1, True)).alias("nb_conformes"),
    count(when(col("is_conforme") == 0, True)).alias("nb_non_conformes")
)

df_result = df_grouped.withColumn(
    "taux_conformite",
    col("nb_conformes") / col("nb_prelevements") * 100
)

df_result.write.mode("overwrite").parquet("data/gold/pyspark/kpi_commune")

print("✅ Gold PySpark OK")