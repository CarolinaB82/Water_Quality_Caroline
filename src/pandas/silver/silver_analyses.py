import pandas as pd
import os

from src.pandas.utils.cleaning import (
    clean_text_columns,
    uppercase_columns,
    titlecase_columns,
    clean_date_columns,
    drop_missing_required
)


def run_silver_analyses():
    print("🧼 Création et nettoyage silver_analyses")

    input_path = "data/raw/DIS_PLV_2024.txt"
    output_path = "data/pandas/silver/analyses/analyses.csv"

    os.makedirs("data/pandas/silver/analyses", exist_ok=True)

    df = pd.read_csv(input_path, sep=",", dtype=str)

    df = df.rename(columns={
        "cddept": "code_departement",
        "cdreseau": "code_reseau",
        "inseecommuneprinc": "code_commune",
        "nomcommuneprinc": "nom_commune",
        "referenceprel": "id_prelevement",
        "dateprel": "date_prelevement",
        "conclusionprel": "conclusion",
        "plvconformitebacterio": "conformite_bacterio",
        "plvconformitechimique": "conformite_chimique"
    })

    df = clean_text_columns(df)
    df = uppercase_columns(df, [
        "code_departement",
        "code_reseau",
        "code_commune",
        "id_prelevement",
        "conformite_bacterio",
        "conformite_chimique"
    ])
    df = titlecase_columns(df, ["nom_commune"])
    df = clean_date_columns(df, ["date_prelevement"])

    df = drop_missing_required(df, [
        "id_prelevement",
        "date_prelevement",
        "code_commune",
        "code_reseau"
    ])

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    df.to_csv(output_path, index=False, sep=";")

    print(f"✅ Silver analyses créé : {output_path}")
    print("Shape :", df.shape)