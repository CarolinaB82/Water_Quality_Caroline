from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import glob

spark = (
    SparkSession.builder
    .appName("WaterQuality-Bronze")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )
    .config(
        "spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter"
    )
    .getOrCreate()
)

# Fix local Windows / Hadoop
spark._jsc.hadoopConfiguration().set(
    "fs.file.impl",
    "org.apache.hadoop.fs.LocalFileSystem"
)

files = glob.glob("data/raw/DIS_PLV_*.txt")

if not files:
    raise FileNotFoundError("Aucun fichier trouvé avec le pattern data/raw/DIS_PLV_*.txt")

df_list = []

for file in files:
    year = file.split("_")[-1].split(".")[0]

    df = (
        spark.read
        .option("header", True)
        .option("delimiter", ",")
        .option("inferSchema", True)
        .csv(file)
        .withColumn("annee_fichier", lit(int(year)))
    )

    df_list.append(df)

df_final = df_list[0]

for df in df_list[1:]:
    df_final = df_final.unionByName(df)

df_final.coalesce(1).write.mode("overwrite").parquet(
    "data/bronze/pyspark/analyses_test"
)

print("✅ Bronze PySpark OK")

spark.stop()