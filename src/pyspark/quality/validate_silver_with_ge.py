import great_expectations as gx

from src.pyspark.config.spark_session import get_spark_session


def validate_silver_analyses(spark):
    print("🔎 Great Expectations - silver_analyses")

    df = spark.read.parquet("data/silver/pyspark/analyses")

    context = gx.get_context()

    data_source = context.data_sources.add_spark(
        name="spark_silver_analyses"
    )

    data_asset = data_source.add_dataframe_asset(
        name="silver_analyses_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "silver_analyses_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id_prelevement"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="code_commune"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="code_reseau"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="date_prelevement"),
        gx.expectations.ExpectColumnValuesToBeInSet(
        column="conformite_bacterio",
        value_set=["C", "N", "S", None]
        ),  
        gx.expectations.ExpectColumnValuesToBeInSet(
        column="conformite_chimique",
        value_set=["C", "N", "S", "D", None]
        ),  
    ]

    success = True

    for expectation in expectations:
        result = batch.validate(expectation)
        if not result.success:
            success = False
            print(f"❌ Échec : {expectation}")

    if not success:
        raise ValueError("❌ Great Expectations échoué sur silver_analyses")

    print("✅ Great Expectations OK : silver_analyses")


def run_great_expectations_checks():
    spark = get_spark_session("WaterQuality-GreatExpectations")

    try:
        validate_silver_analyses(spark)
        print("✅ Tous les contrôles Great Expectations sont validés")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_great_expectations_checks()