import pandas as pd
import os


def run_build_kpis():
    print("🥇 Construction KPIs")

    os.makedirs("data/gold/kpis", exist_ok=True)

    df = pd.read_csv("data/gold/facts/fact_conformite.csv", sep=";", dtype=str)

    df["is_conforme"] = df["is_conforme"].astype(int)
    df["is_non_conforme"] = df["is_non_conforme"].astype(int)

    kpi_commune = (
        df.groupby(["code_departement", "code_commune", "commune", "annee_fichier"])
        .agg(
            nb_prelevements=("id_prelevement", "count"),
            nb_conformes=("is_conforme", "sum"),
            nb_non_conformes=("is_non_conforme", "sum")
        )
        .reset_index()
    )

    kpi_commune["taux_conformite"] = (
        kpi_commune["nb_conformes"] / kpi_commune["nb_prelevements"] * 100
    ).round(2)

    kpi_commune.to_csv("data/gold/kpis/kpi_conformite_commune.csv", index=False, sep=";")

    kpi_departement = (
        df.groupby(["code_departement", "annee_fichier"])
        .agg(
            nb_prelevements=("id_prelevement", "count"),
            nb_conformes=("is_conforme", "sum"),
            nb_non_conformes=("is_non_conforme", "sum")
        )
        .reset_index()
    )

    kpi_departement["taux_conformite"] = (
        kpi_departement["nb_conformes"] / kpi_departement["nb_prelevements"] * 100
    ).round(2)

    kpi_departement.to_csv("data/gold/kpis/kpi_conformite_departement.csv", index=False, sep=";")

    print("✅ KPIs Gold créés")