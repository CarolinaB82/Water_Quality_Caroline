from pyspark.sql.functions import col, trim, upper, initcap, to_timestamp

from src.pyspark.config.spark_session import get_spark_session


def rename_if_exists(df, old_name, new_name):
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)
    return df


def run_silver_analyses():
    spark = get_spark_session("WaterQuality-Silver-Analyses")

    try:
        input_path = "data/pyspark/bronze/analyses"
        output_path = "data/pyspark/silver/analyses"

        df = spark.read.parquet(input_path)

        rename_mapping = {
            "cddept": "code_departement",
            "cdreseau": "code_reseau",
            "inseecommuneprinc": "code_commune",
            "nomcommuneprinc": "nom_commune",
            "referenceprel": "id_prelevement",
            "dateprel": "date_prelevement",
            "conclusionprel": "conclusion",
            "plvconformitebacterio": "conformite_bacterio",
            "plvconformitechimique": "conformite_chimique",
        }

        for old_name, new_name in rename_mapping.items():
            df = rename_if_exists(df, old_name, new_name)

        for c in [
            "code_departement",
            "code_reseau",
            "code_commune",
            "id_prelevement",
            "conformite_bacterio",
            "conformite_chimique",
        ]:
            if c in df.columns:
                df = df.withColumn(c, upper(trim(col(c))))

        if "nom_commune" in df.columns:
            df = df.withColumn("nom_commune", initcap(trim(col("nom_commune"))))

        if "date_prelevement" in df.columns:
            df = df.withColumn(
                "date_prelevement",
                to_timestamp(col("date_prelevement"))
            )

        required_columns = [
            "id_prelevement",
            "date_prelevement",
            "code_commune",
            "code_reseau",
        ]

        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Colonnes manquantes après renommage : {missing}")

        for c in required_columns:
            df = df.filter(col(c).isNotNull())

        df = df.dropDuplicates(required_columns)

        df.write.mode("overwrite").parquet(output_path)

        print(f"✅ Silver analyses PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_analyses()