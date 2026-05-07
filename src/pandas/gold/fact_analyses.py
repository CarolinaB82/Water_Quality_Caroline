import pandas as pd
import os


def run_fact_analyses():
    print("🥇 Création fact_analyses")

    analyses_path = "data/pandas/silver/analyses/analyses.csv"
    parametres_path = "data/pandas/silver/parametres/parametres.csv"
    dim_temps_path = "data/pandas/gold/dim_temps/dim_temps.csv"

    output_path = "data/pandas/gold/fact_analyses/fact_analyses.csv"

    os.makedirs("data/pandas/gold/fact_analyses", exist_ok=True)

    analyses = pd.read_csv(analyses_path, sep=";", dtype=str)
    parametres = pd.read_csv(parametres_path, sep=";", dtype=str)
    dim_temps = pd.read_csv(dim_temps_path, sep=";", dtype=str)

    analyses["date_prelevement"] = pd.to_datetime(
        analyses["date_prelevement"],
        errors="coerce"
    )

    dim_temps["date"] = pd.to_datetime(
        dim_temps["date"],
        errors="coerce"
    )

    def definir_statut_conformite(row):
        valeurs = [
            row.get("conformite_bacterio"),
            row.get("conformite_chimique")
        ]

        if "N" in valeurs:
            return "non_conforme"

        if "C" in valeurs:
            return "conforme"

        return "non_evalue"

    analyses["statut_conformite"] = analyses.apply(
        definir_statut_conformite,
        axis=1
    )

    parametres_fact = parametres[
        [
            "id_prelevement",
            "parametres_id",
            "valeur_traduite"
        ]
    ].copy()

    parametres_fact = parametres_fact.rename(
        columns={
            "valeur_traduite": "resultat_numerique"
        }
    )

    fact = analyses.merge(
        parametres_fact,
        on="id_prelevement",
        how="left"
    )

    fact = fact.merge(
        dim_temps[["date_id", "date"]],
        left_on="date_prelevement",
        right_on="date",
        how="left"
    )

    fact = fact[
        [
            "id_prelevement",
            "code_commune",
            "code_reseau",
            "parametres_id",
            "resultat_numerique",
            "statut_conformite",
            "date_id"
        ]
    ]

    fact = fact.rename(
        columns={
            "id_prelevement": "code_prelevement"
        }
    )

    fact["resultat_numerique"] = (
        fact["resultat_numerique"]
        .astype(str)
        .str.replace(",", ".", regex=False)
    )

    fact["resultat_numerique"] = pd.to_numeric(
        fact["resultat_numerique"],
        errors="coerce"
    )

    fact = fact.dropna(
        subset=[
            "code_prelevement",
            "code_commune",
            "code_reseau",
            "date_id"
        ]
    )

    fact = fact.drop_duplicates()
    fact = fact.reset_index(drop=True)

    fact.to_csv(output_path, index=False, sep=";")

    print(f"✅ fact_analyses créé : {output_path}")
    print("Shape :", fact.shape)