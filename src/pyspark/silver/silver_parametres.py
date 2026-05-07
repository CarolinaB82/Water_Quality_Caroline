from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    row_number
)
from pyspark.sql.window import Window

from src.pyspark.config.spark_session import get_spark_session


def run_silver_parametres():
    spark = get_spark_session("WaterQuality-Silver-Parametres")

    try:
        input_path = "data/pyspark/bronze/parametres"
        output_path = "data/pyspark/silver/parametres"

        df = spark.read.parquet(input_path)

        df = (
            df
            .withColumnRenamed("cddept", "code_departement")
            .withColumnRenamed("referenceprel", "id_prelevement")
            .withColumnRenamed("cdparametresiseeaux", "code_parametresiseeaux")
            .withColumnRenamed("cdparametre", "code_parametre")
            .withColumnRenamed("libmajparametre", "libelle_parametre_maj")
            .withColumnRenamed("libminparametre", "libelle_parametre_min")
            .withColumnRenamed("libwebparametre", "libelle_parametre_web")
            .withColumnRenamed("qualitparam", "qualite_parametre")
            .withColumnRenamed("insituana", "insitu_ana")
            .withColumnRenamed("rqana", "rq_ana")
            .withColumnRenamed(
                "cdunitereferencesiseeaux",
                "code_unite_ref_siseeaux"
            )
            .withColumnRenamed("cdunitereference", "code_unite_ref")
            .withColumnRenamed(
                "limitequal",
                "limite_qualite_parametre"
            )
            .withColumnRenamed(
                "refqual",
                "reference_qualite_parametre"
            )
            .withColumnRenamed("valtraduite", "valeur_traduite")
            .withColumnRenamed("casparam", "cas_param")
            .withColumnRenamed("referenceanl", "reference_analyse")
        )

        for c in [
            "code_departement",
            "id_prelevement",
            "code_parametre",
            "code_parametresiseeaux",
            "code_unite_ref",
            "code_unite_ref_siseeaux",
            "reference_analyse"
        ]:
            if c in df.columns:
                df = df.withColumn(c, upper(trim(col(c))))

        for c in [
            "libelle_parametre_min",
            "libelle_parametre_web"
        ]:
            if c in df.columns:
                df = df.withColumn(c, initcap(trim(col(c))))

        for c in [
            "id_prelevement",
            "code_parametre",
            "reference_analyse"
        ]:
            if c in df.columns:
                df = df.filter(col(c).isNotNull())

        df = df.dropDuplicates([
            "id_prelevement",
            "code_parametre",
            "reference_analyse"
        ])

        window_spec = Window.orderBy("code_parametre")

        df = df.withColumn(
            "parametres_id",
            row_number().over(window_spec)
        )

        cols = [
            "parametres_id"
        ] + [
            c for c in df.columns if c != "parametres_id"
        ]

        df = df.select(*cols)

        df.write \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"✅ Silver parametres PySpark créé : {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_parametres()