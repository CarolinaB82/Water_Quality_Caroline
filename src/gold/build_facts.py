import pandas as pd
import os


def run_build_facts():
    print("🥇 Construction tables de faits")

    os.makedirs("data/gold/facts", exist_ok=True)

    conformite = pd.read_csv("data/silver/conformite/conformite.csv", sep=";", dtype=str)

    fact_conformite = conformite[[
        "id_prelevement",
        "code_departement",
        "code_commune",
        "commune",
        "date_prelevement",
        "is_conforme",
        "is_non_conforme",
        "annee_fichier"
    ]].copy()

    fact_conformite.to_csv("data/gold/facts/fact_conformite.csv", index=False, sep=";")

    print("✅ Table de faits conformité créée")