import pandas as pd
import glob
import os


def run_ingestion_stations():
    print("🚀 Ingestion STATIONS / COMMUNES")

    os.makedirs("data/pandas/bronze/stations", exist_ok=True)

    files = glob.glob("data/raw/DIS_COM_UDI_*.txt")
    dfs = []

    for file in files:
        print(f"Lecture : {file}")
        df = pd.read_csv(file, sep=",", dtype=str)
        annee = file.split("_")[-1].replace(".txt", "")
        df["annee_fichier"] = annee
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    output_path = "data/pandas/bronze/stations/stations.csv"
    df_final.to_csv(output_path, index=False, sep=";")

    print(f"✅ Bronze STATIONS créé : {output_path}")