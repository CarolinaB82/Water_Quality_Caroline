# 💧 Water Quality Data Pipeline (France)

Projet de data engineering dédié à l’analyse de la qualité de l’eau potable en France, basé sur les données du portail Hub’Eau.

---

## Trois pipelines : Pandas, PySpark, Databricks Free Edition

Ce projet propose trois modes d’exécution pour traiter les données :

### 1. Pipeline Pandas (local)
- Les données brutes (fichiers .txt DIS_PLV, DIS_RESULT, DIS_COM_UDI) sont ingérées, nettoyées et transformées avec Pandas.
- Les résultats sont produits au format CSV dans data/pandas/ (bronze, silver, gold).
- Orchestration via le script `main.py`.

**Utilisation :**
```bash
python main.py
```

### 2. Pipeline PySpark (local ou Databricks)
- Les mêmes fichiers .txt sont utilisés comme source.
- Le traitement s’effectue avec PySpark, pour un passage facile à Databricks ou à un environnement distribué.
- Les résultats sont produits au format Parquet dans data/pyspark/ (bronze, silver, gold).
- Orchestration via le script `src/pyspark/main_pyspark.py`.

**Utilisation :**
```bash
python src/pyspark/main_pyspark.py
```

### 3. Pipeline Databricks Free Edition (déclenché par API)
- Un pipeline Databricks peut être déclenché à distance via une API FastAPI/Uvicorn.
- L’API appelle un job Databricks défini dans le cloud (Databricks Community/Free Edition).
- Les variables d’environnement (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID) sont à placer dans un fichier `.env` à la racine du projet.
- Orchestration via le script `api_databricks_free_edition/deployment_api.py`.

**Utilisation :**
```bash
uvicorn api_databricks_free_edition.deployment_api:app --reload
```

---

## Données sources
- Les fichiers .txt doivent être placés dans `data/raw/`.
- Les trois pipelines utilisent ces mêmes fichiers comme point de départ.

---

## Analyse et KPI avec DuckDB
- Les fichiers Parquet produits par PySpark (dans `data/pyspark/gold/`) peuvent être analysés avec DuckDB.
- Place tes requêtes SQL dans le dossier `sql/` et exécute-les via `query_duckdb.py`.
- Exemples de requêtes pour KPI : nombre d’analyses par commune, taux de conformité, top non-conformités, etc.

---

## Structure du projet (extrait)

src/
├── pandas/
│   ├── bronze/ (ingestion)
│   ├── silver/ (nettoyage)
│   ├── gold/ (modélisation)
│   ├── quality/ (contrôles)
│   └── utils/
├── pyspark/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quality/
│   ├── config/
│   └── utils/
└── utils/

data/
├── raw/
├── pandas/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── pyspark/
    ├── bronze/
    ├── silver/
    └── gold/

api_databricks_free_edition/
└── deployment_api.py

---

## Installation & initialisation

1. Installer les dépendances :
```bash
pip install -r requirements.txt
```
2. Générer la structure du projet (optionnel) :
```bash
python setup_project.py
```

---

## Résumé
- Trois pipelines (Pandas, PySpark, Databricks Free Edition) exploitant les mêmes données brutes.
- Résultats exploitables en CSV (pandas) ou Parquet (pyspark/databricks).
- Analyse possible avec DuckDB ou Databricks SQL.
- API de déploiement pour Databricks (FastAPI/Uvicorn).
- Architecture médallion (bronze, silver, gold).
- Contrôles de qualité intégrés.

Pour plus de détails, voir la documentation dans le dossier `docs/`.