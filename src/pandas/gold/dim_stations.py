import pandas as pd
import os


def run_dim_stations():
    print("🥇 Création dim_stations")

    input_path = "data/pandas/silver/stations/stations.csv"
    output_path = "data/pandas/gold/dim_stations/dim_stations.csv"

    os.makedirs("data/pandas/gold/dim_stations", exist_ok=True)

    df = pd.read_csv(input_path, sep=";", dtype=str)

    columns_to_drop = [
        "source_api",
        "ingestion_timestamp",
        "page_api"
    ]

    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns])

    df = df.drop_duplicates()
    df.to_csv(output_path, index=False, sep=";")

    print(f"✅ dim_stations créé : {output_path}")
    print("Shape :", df.shape)