import pandas as pd
import os


def run_clean_mesures():
    print("🧼 Nettoyage MESURES")

    input_path = "data/bronze/analyses/analyses.csv"
    output_path = "data/silver/mesures/mesures.csv"

    os.makedirs("data/silver/mesures", exist_ok=True)

    df = pd.read_csv(input_path, sep=";", dtype=str)

    df_clean = df[[
        "referenceprel",
        "cddept",
        "cdreseau",
        "inseecommuneprinc",
        "nomcommuneprinc",
        "dateprel",
        "conclusionprel",
        "plvconformitebacterio",
        "plvconformitechimique",
        "annee_fichier"
    ]].copy()

    df_clean = df_clean.rename(columns={
        "referenceprel": "id_prelevement",
        "cddept": "code_departement",
        "cdreseau": "code_reseau",
        "inseecommuneprinc": "code_commune",
        "nomcommuneprinc": "commune",
        "dateprel": "date_prelevement",
        "conclusionprel": "conclusion",
        "plvconformitebacterio": "conformite_bacterio",
        "plvconformitechimique": "conformite_chimique"
    })

    df_clean = df_clean.dropna(subset=["id_prelevement", "commune", "date_prelevement"])
    df_clean = df_clean.drop_duplicates()

    df_clean["conformite_globale"] = (
        (df_clean["conformite_bacterio"] == "C") &
        (df_clean["conformite_chimique"] == "C")
    ).map({
        True: "Conforme",
        False: "Non conforme"
    })

    df_clean.to_csv(output_path, index=False, sep=";")

    print(f"✅ Silver mesures créé : {output_path}")