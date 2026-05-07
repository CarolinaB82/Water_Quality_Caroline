from pyspark.sql.functions import col, to_date


def check_required_columns(df, required_columns: list, table_name: str):
    missing = [c for c in required_columns if c not in df.columns]

    if missing:
        raise ValueError(f"{table_name} : colonnes manquantes {missing}")

    print(f"✅ {table_name} : colonnes obligatoires OK")


def check_not_empty(df, table_name: str):
    if df.limit(1).count() == 0:
        raise ValueError(f"{table_name} : table vide")

    print(f"✅ {table_name} : table non vide OK")


def check_no_duplicates(df, table_name: str, subset=None):
    if subset is None:
        subset = df.columns

    total = df.count()
    distinct = df.dropDuplicates(subset).count()
    duplicates = total - distinct

    if duplicates > 0:
        print(f"⚠️ {table_name} : {duplicates} doublons détectés")
    else:
        print(f"✅ {table_name} : aucun doublon")


def check_not_null(df, columns: list, table_name: str):
    for c in columns:
        if c in df.columns:
            null_count = df.filter(col(c).isNull()).count()

            if null_count > 0:
                raise ValueError(
                    f"{table_name} : {null_count} valeurs nulles dans {c}"
                )

    print(f"✅ {table_name} : colonnes clés non nulles OK")


def check_values_in_set(df, column: str, allowed_values: list, table_name: str):
    if column not in df.columns:
        print(f"⚠️ {table_name} : colonne absente {column}")
        return

    invalid_count = (
        df
        .filter(col(column).isNotNull())
        .filter(~col(column).isin(allowed_values))
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{table_name} : {invalid_count} valeurs invalides dans {column}"
        )

    print(f"✅ {table_name} : valeurs autorisées OK pour {column}")


def check_date_column(df, column: str, table_name: str):
    if column not in df.columns:
        print(f"⚠️ {table_name} : colonne absente {column}")
        return

    invalid_count = (
        df
        .filter(col(column).isNotNull())
        .filter(to_date(col(column)).isNull())
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{table_name} : {invalid_count} dates invalides dans {column}"
        )

    print(f"✅ {table_name} : dates valides pour {column}")