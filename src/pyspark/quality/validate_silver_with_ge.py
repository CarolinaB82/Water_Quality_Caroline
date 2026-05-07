# En lien avec mon .env, j'ai ajouté les variables d'environnement pour Databricks afin de pouvoir exécuter le pipeline PySpark directement depuis mon environnement local. Cela me permettra de lancer les scripts PySpark qui se connectent à Databricks sans



import great_expectations as gx

from src.pyspark.config.spark_session import get_spark_session



def validate_silver_table(spark, parquet_path, table_name, expectations):
    print(f"🔎 Great Expectations - {table_name}")

    df = spark.read.parquet(parquet_path)
    context = gx.get_context()
    data_source = context.data_sources.add_spark(name=f"spark_{table_name}")
    data_asset = data_source.add_dataframe_asset(name=f"{table_name}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{table_name}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    success = True
    for expectation in expectations:
        result = batch.validate(expectation)
        if not result.success:
            success = False
            print(f"❌ Échec : {expectation}")

    if not success:
        raise ValueError(f"❌ Great Expectations échoué sur {table_name}")

    print(f"✅ Great Expectations OK : {table_name}")



def run_great_expectations_checks():
    spark = get_spark_session("WaterQuality-GreatExpectations")
    try:
        # Contrôles pour silver_analyses
        validate_silver_table(
            spark,
            "data/pyspark/silver/analyses",
            "silver_analyses",
            [
                gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="id_prelevement"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="code_commune"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="code_reseau"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="date_prelevement"),
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column="conformite_bacterio", value_set=["C", "N", "S", None]
                ),
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column="conformite_chimique", value_set=["C", "N", "S", "D", None]
                ),
            ]
        )

        # Contrôles pour silver_parametres
        validate_silver_table(
            spark,
            "data/pyspark/silver/parametres",
            "silver_parametres",
            [
                gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="id_prelevement"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="code_parametre"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="code_parametresiseeaux"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="libelle_parametre_maj"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="libelle_parametre_min"),
            ]
        )

        # Contrôles pour silver_stations
        validate_silver_table(
            spark,
            "data/pyspark/silver/stations",
            "silver_stations",
            [
                gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="stations_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="code_commune"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="nom_commune"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="code_reseau"),
            ]
        )

        print("✅ Tous les contrôles Great Expectations sont validés")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_great_expectations_checks()