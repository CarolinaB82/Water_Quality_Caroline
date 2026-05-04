import pandas as pd
import os


def run_build_dimensions():
    print("🥇 Construction dimensions Gold")

    os.makedirs("data/gold/dimensions", exist_ok=True)

    mesures = pd.read_csv("data/silver/mesures/mesures.csv", sep=";", dtype=str)

    dim_communes = mesures[[
        "code_departement",
        "code_commune",
        "commune"
    ]].drop_duplicates()

    dim_temps = mesures[[
        "date_prelevement",
        "annee_fichier"
    ]].drop_duplicates()

    dim_temps["mois"] = dim_temps["date_prelevement"].str.slice(5, 7)

    dim_communes.to_csv("data/gold/dimensions/dim_communes.csv", index=False, sep=";")
    dim_temps.to_csv("data/gold/dimensions/dim_temps.csv", index=False, sep=";")

    print("✅ Dimensions Gold créées")