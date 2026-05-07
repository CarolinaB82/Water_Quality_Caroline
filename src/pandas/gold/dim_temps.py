import pandas as pd
import os


def run_dim_temps():
    print("🥇 Création dim_temps")

    input_path = "data/pandas/silver/analyses/analyses.csv"
    output_path = "data/pandas/gold/dim_temps/dim_temps.csv"

    os.makedirs("data/pandas/gold/dim_temps", exist_ok=True)

    df = pd.read_csv(input_path, sep=";", dtype=str)

    df["date_prelevement"] = pd.to_datetime(
        df["date_prelevement"],
        errors="coerce"
    )

    dim_temps = (
        df[["date_prelevement"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"date_prelevement": "date"})
        .sort_values("date")
        .reset_index(drop=True)
    )

    dim_temps["date_id"] = dim_temps.index + 1
    dim_temps["annee"] = dim_temps["date"].dt.year
    dim_temps["mois"] = dim_temps["date"].dt.month
    dim_temps["jour"] = dim_temps["date"].dt.day
    dim_temps["trimestre"] = dim_temps["date"].dt.quarter

    dim_temps = dim_temps[
        [
            "date_id",
            "date",
            "annee",
            "mois",
            "jour",
            "trimestre"
        ]
    ]

    dim_temps.to_csv(output_path, index=False, sep=";")

    print(f"✅ dim_temps créé : {output_path}")
    print("Shape :", dim_temps.shape)