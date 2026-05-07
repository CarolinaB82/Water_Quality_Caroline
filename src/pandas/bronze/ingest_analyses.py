import pandas as pd
import glob
import os

def run_ingestion_analyses():
    print("🚀 Ingestion ANALYSES (PLV)")

    os.makedirs("data/pandas/bronze/analyses", exist_ok=True)

    files = glob.glob("data/raw/DIS_PLV_*.txt")

    dfs = []

    for file in files:
        print(f"Lecture : {file}")

        df = pd.read_csv(file, sep=",", dtype=str)

        # ajouter année
        annee = file.split("_")[-1].replace(".txt", "")
        df["annee_fichier"] = annee

        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    output_path = "data/pandas/bronze/analyses/analyses.csv"
    df_final.to_csv(output_path, index=False, sep=";")

    print(f"✅ Bronze ANALYSES créé : {output_path}")