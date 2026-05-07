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
- Les résultats sont produits dans data/pyspark/ (bronze, silver, gold).
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

## Requêtes analytiques
- Les résultats produits (CSV ou Parquet) peuvent être analysés avec DuckDB (local) ou Databricks SQL.
- Les requêtes SQL sont dans le dossier `sql/`.

---

## Résumé
- Deux pipelines (Pandas et PySpark) exploitant les mêmes données brutes.
- Adapté à un usage local ou cloud (Databricks).
- Architecture médallion (bronze, silver, gold).
- Contrôles de qualité intégrés.

Pour plus de détails, voir la documentation dans le dossier `docs/`.