🏗️ Architecture du pipeline Data Engineering – Qualité de l’eau

🎯 Objectif

Mettre en place un pipeline de données moderne basé sur l’architecture médallion (Bronze, Silver, Gold) afin de traiter et analyser la qualité de l’eau en France.

🔀 Vue globale du pipeline

Sources (Hub’Eau / fichiers)
        ↓
     Bronze
        ↓
     Silver
        ↓
      Gold
        ↓
Analyse SQL (DuckDB ou Databricks SQL)
        ↓
Insights métiers


🚦 Trois pipelines disponibles

- Pipeline Pandas (local, CSV)
- Pipeline PySpark (local ou Databricks, Parquet)
- Pipeline Databricks Free Edition (déclenché par API FastAPI/Uvicorn)


🟫 Couche Bronze — Ingestion

📥 Données sources
DIS_PLV : prélèvements / analyses
DIS_RESULT : résultats des paramètres physico-chimiques
DIS_COM_UDI : informations sur les communes et réseaux

⚙️ Traitement
lecture des fichiers .txt
concaténation multi-années (2024, 2025…)
ajout de la colonne annee_fichier
stockage brut sans transformation

📁 Sortie
- data/pandas/bronze/
- data/pyspark/bronze/


⬜ Couche Silver — Nettoyage & Transformation

🧼 Traitements appliqués
suppression des valeurs nulles critiques
suppression des doublons
sélection et renommage des colonnes utiles
standardisation des formats

🔍 Enrichissement
création d’un indicateur de conformité globale
séparation des données métiers (analyses, paramètres, stations)

📁 Sortie
- data/pandas/silver/
- data/pyspark/silver/


🟨 Couche Gold — Modélisation & Analyse

📊 Structuration pour l’analyse (dimensions, faits, KPIs)

📁 Sortie
- data/pandas/gold/
- data/pyspark/gold/


🌐 API de déploiement (Databricks Free Edition)
- Déclenchement du pipeline cloud via FastAPI/Uvicorn
- Variables d’environnement dans .env
- Orchestration dans api_databricks_free_edition/


🔎 Analyse & KPI
- Requêtes SQL sur les fichiers Parquet (PySpark/Databricks) ou CSV (Pandas)
- Utilisation de DuckDB ou Databricks SQL
- Exemples : taux de conformité, top non-conformités, analyses par commune…