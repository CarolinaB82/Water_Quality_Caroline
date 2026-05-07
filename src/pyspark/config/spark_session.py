from pyspark.sql import SparkSession


def get_spark_session(app_name="WaterQuality"):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2"
        )
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

    spark.sparkContext.setLogLevel("WARN")

    spark._jsc.hadoopConfiguration().set(
        "fs.file.impl",
        "org.apache.hadoop.fs.LocalFileSystem"
    )

    return spark