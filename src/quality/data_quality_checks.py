import pandas as pd


def check_required_columns(df: pd.DataFrame, required_columns: list, table_name: str):
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"{table_name} : colonnes manquantes {missing}")

    print(f"✅ {table_name} : colonnes obligatoires OK")


def check_not_empty(df: pd.DataFrame, table_name: str):
    if df.empty:
        raise ValueError(f"{table_name} : table vide")

    print(f"✅ {table_name} : table non vide OK")


def check_no_duplicates(df: pd.DataFrame, table_name: str):
    duplicates = df.duplicated().sum()

    if duplicates > 0:
        print(f"⚠️ {table_name} : {duplicates} doublons détectés")
    else:
        print(f"✅ {table_name} : aucun doublon")