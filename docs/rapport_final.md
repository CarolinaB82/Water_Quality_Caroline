# Rapport Final — Water Quality Data Engineering

## Présentation du projet

Ce projet vise à construire un pipeline de données robuste pour l’analyse de la qualité de l’eau potable en France, en s’appuyant sur l’architecture médallion (Bronze, Silver, Gold) et en offrant trois modes d’exécution :
- Pipeline Pandas (local, CSV)
- Pipeline PySpark (local ou Databricks, Parquet)
- Pipeline Databricks Free Edition (déclenché par API)

## Objectifs
- Automatiser l’ingestion, le nettoyage et la transformation des données environnementales.
- Produire des indicateurs analytiques (KPIs) sur la qualité de l’eau.
- Permettre l’analyse locale (DuckDB) ou cloud (Databricks SQL).
- Offrir une API de déploiement pour industrialiser le pipeline.

## Architecture
- **Bronze** : Ingestion brute des fichiers .txt (DIS_PLV, DIS_RESULT, DIS_COM_UDI)
- **Silver** : Nettoyage, enrichissement, structuration (analyses, paramètres, stations)
- **Gold** : Modélisation analytique (dimensions, faits, KPIs)
- **API** : Déclenchement du pipeline Databricks à distance

## Fonctionnement
1. Dépôt des fichiers bruts dans `data/raw/`
2. Exécution d’un des trois pipelines selon le besoin
3. Résultats produits en CSV (pandas) ou Parquet (pyspark/databricks)
4. Analyse des résultats avec DuckDB ou Databricks SQL
5. Déploiement cloud possible via l’API FastAPI/Uvicorn

## Exemples de KPI produits
- Nombre d’analyses par commune et par an
- Taux de conformité global et par paramètre
- Top 10 des communes non conformes
- Évolution temporelle de la qualité de l’eau

## Points forts
- Flexibilité locale/cloud
- Architecture claire et scalable
- Contrôles de qualité intégrés (Great Expectations)
- Documentation et dictionnaire de données fournis

## Améliorations possibles
- Intégration temps réel via API Hub’Eau
- Orchestration avancée (Airflow, Databricks Workflows)
- Visualisation interactive (Power BI, Databricks SQL)

---

Pour toute question ou contribution, voir la documentation détaillée dans le dossier `docs/`.
