import pandas as pd
import os


def run_build_conformite():
    print("🧼 Construction conformité")

    input_path = "data/silver/mesures/mesures.csv"
    output_path = "data/silver/conformite/conformite.csv"

    os.makedirs("data/silver/conformite", exist_ok=True)

    df = pd.read_csv(input_path, sep=";", dtype=str)

    df_conf = df[[
        "id_prelevement",
        "code_departement",
        "code_commune",
        "commune",
        "date_prelevement",
        "conformite_bacterio",
        "conformite_chimique",
        "conformite_globale",
        "annee_fichier"
    ]].copy()

    df_conf["is_conforme"] = df_conf["conformite_globale"].eq("Conforme").astype(int)
    df_conf["is_non_conforme"] = df_conf["conformite_globale"].eq("Non conforme").astype(int)

    df_conf.to_csv(output_path, index=False, sep=";")

    print(f"✅ Silver conformité créé : {output_path}")