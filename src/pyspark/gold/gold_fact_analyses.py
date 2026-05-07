from pyspark.sql.functions import (
    col,
    when,
    to_date,
    regexp_replace
)

from src.pyspark.config.spark_session import get_spark_session


def run_gold_fact_analyses():
    spark = get_spark_session("WaterQuality-Gold-FactAnalyses")

    try:
        analyses_path = "data/pyspark/silver/analyses"
        parametres_path = "data/pyspark/silver/parametres"
        dim_temps_path = "data/pyspark/gold/dim_temps"
        output_path = "data/pyspark/gold/fact_analyses"

        analyses = spark.read.parquet(analyses_path)
        parametres = spark.read.parquet(parametres_path)
        dim_temps = spark.read.parquet(dim_temps_path)

        analyses = analyses.withColumn(
            "statut_conformite",
            when(
                (col("conformite_bacterio") == "N") |
                (col("conformite_chimique") == "N"),
                "non_conforme"
            )
            .when(
                (col("conformite_bacterio") == "C") |
                (col("conformite_chimique") == "C"),
                "conforme"
            )
            .otherwise("non_evalue")
        )

        parametres_fact = (
            parametres
            .select(
                "id_prelevement",
                "parametres_id",
                "valeur_traduite"
            )
            .withColumnRenamed("valeur_traduite", "resultat_numerique")
            .dropDuplicates([
                "id_prelevement",
                "parametres_id"
            ])
        )

        fact = (
            analyses
            .join(
                parametres_fact,
                on="id_prelevement",
                how="left"
            )
        )

        fact = fact.withColumn(
            "date_join",
            to_date(col("date_prelevement"))
        )

        dim_temps = dim_temps.withColumn(
            "date_join",
            to_date(col("date"))
        )

        fact = (
            fact
            .join(
                dim_temps.select("date_join", "date_id"),
                on="date_join",
                how="left"
            )
        )

        fact = fact.select(
            col("id_prelevement").alias("code_prelevement"),
            "code_commune",
            "code_reseau",
            "parametres_id",
            regexp_replace(
                col("resultat_numerique"),
                ",",
                "."
            ).cast("double").alias("resultat_numerique"),
            "statut_conformite",
            "date_id"
        )

        fact = fact.filter(col("code_prelevement").isNotNull())
        fact = fact.filter(col("code_commune").isNotNull())
        fact = fact.filter(col("code_reseau").isNotNull())
        fact = fact.filter(col("date_id").isNotNull())

        fact = fact.dropDuplicates([
            "code_prelevement",
            "parametres_id",
            "date_id"
        ])

        fact.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Gold fact_analyses PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_fact_analyses()