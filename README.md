# 💧 Water Quality Data Pipeline (France)

Projet de data engineering dédié à l’analyse de la qualité de l’eau potable en France, basé sur les données du portail Hub’Eau.

---

## Deux pipelines : Pandas & PySpark

Ce projet propose deux modes d’exécution pour traiter les données :

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

---

## Données sources
- Les fichiers .txt doivent être placés dans `data/raw/`.
- Les deux pipelines utilisent ces mêmes fichiers comme point de départ.

---

## Analyse et KPI avec DuckDB
- Les fichiers Parquet produits par PySpark (dans `data/pyspark/gold/`) peuvent être analysés avec DuckDB.
- Place tes requêtes SQL dans le dossier `sql/` et exécute-les via `query_duckdb.py`.
- Exemples de requêtes pour KPI : nombre d’analyses par commune, taux de conformité, top non-conformités, etc.

---

## API de déploiement (FastAPI/Uvicorn)
- Une API FastAPI permet de déclencher le workflow Databricks à distance.
- Les variables d’environnement (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID) sont à placer dans un fichier `.env` à la racine du projet.
- Lancement de l’API :
```bash
uvicorn api_databricks_free_edition.deployment_api:app --reload
```
- Endpoints principaux :
  - `GET /` : Vérification de l’API
  - `POST /deploy` : Déclenche le pipeline Databricks

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
- Deux pipelines (Pandas et PySpark) exploitant les mêmes données brutes.
- Résultats exploitables en CSV (pandas) ou Parquet (pyspark).
- Analyse possible avec DuckDB ou Databricks SQL.
- API de déploiement pour Databricks (FastAPI/Uvicorn).
- Architecture médallion (bronze, silver, gold).
- Contrôles de qualité intégrés.

Pour plus de détails, voir la documentation dans le dossier `docs/`.