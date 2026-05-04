import pandas as pd
import os


def run_clean_stations():
    print("🧼 Nettoyage STATIONS")

    input_path = "data/bronze/stations/stations.csv"
    output_path = "data/silver/stations/stations.csv"

    os.makedirs("data/silver/stations", exist_ok=True)

    df = pd.read_csv(input_path, sep=";", dtype=str)

    cols_to_keep = [
        col for col in [
            "cddept",
            "cdreseau",
            "inseecommune",
            "nomcommune",
            "nomreseau",
            "annee_fichier"
        ]
        if col in df.columns
    ]

    df_clean = df[cols_to_keep].copy()
    df_clean = df_clean.drop_duplicates()

    rename_map = {
        "cddept": "code_departement",
        "cdreseau": "code_reseau",
        "inseecommune": "code_commune",
        "nomcommune": "commune",
        "nomreseau": "reseau"
    }

    df_clean = df_clean.rename(columns=rename_map)

    df_clean.to_csv(output_path, index=False, sep=";")

    print(f"✅ Silver stations créé : {output_path}")