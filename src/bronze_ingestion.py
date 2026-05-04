import pandas as pd
import os
import glob

# Créer dossier Bronze
os.makedirs("data/bronze/plv", exist_ok=True)

# Récupérer tous les fichiers PLV automatiquement
files = glob.glob("data/raw/DIS_PLV_*.txt")

dfs = []

for file in files:
    print(f"Lecture : {file}")
    
    df = pd.read_csv(file, sep=",", dtype=str)
    
    # Extraire l'année depuis le nom du fichier
    annee = file.split("_")[-1].replace(".txt", "")
    df["annee_fichier"] = annee
    
    dfs.append(df)

# Concaténer tous les fichiers
df_final = pd.concat(dfs, ignore_index=True)

print(df_final.head())
print(df_final.columns)
print("Nombre de lignes :", len(df_final))

# Sauvegarde Bronze
output_path = "data/bronze/plv/bronze_plv.csv"
df_final.to_csv(output_path, index=False, sep=";")

print(f"✅ Bronze créé : {output_path}")