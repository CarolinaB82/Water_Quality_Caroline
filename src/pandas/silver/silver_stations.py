import pandas as pd
import os

from src.pandas.utils.cleaning import (
    clean_text_columns,
    uppercase_columns,
    titlecase_columns,
    clean_date_columns,
    drop_missing_required,
    add_sequential_id
)


def run_silver_stations():
    print("🧼 Création et nettoyage silver_stations")

    input_path = "data/raw/DIS_COM_UDI_2024.txt"
    output_path = "data/pandas/silver/stations/stations.csv"

    os.makedirs("data/pandas/silver/stations", exist_ok=True)

    df = pd.read_csv(input_path, sep=",", dtype=str)

    df = df.rename(columns={
        "inseecommune": "code_commune",
        "nomcommune": "nom_commune",
        "quartier": "nom_quartier",
        "cdreseau": "code_reseau",
        "nomreseau": "nom_reseau",
        "debutalim": "debut_alim"
    })

    df = clean_text_columns(df)
    df = uppercase_columns(df, ["code_commune", "code_reseau"])
    df = titlecase_columns(df, ["nom_commune", "nom_quartier", "nom_reseau"])
    df = clean_date_columns(df, ["debut_alim"])

    df = drop_missing_required(df, ["code_commune", "code_reseau"])
    df = df.drop_duplicates(subset=["code_commune", "code_reseau", "debut_alim"])

    df = add_sequential_id(df, "stations_id")

    df.to_csv(output_path, index=False, sep=";")

    print(f"✅ Silver stations créé : {output_path}")
    print("Shape :", df.shape)