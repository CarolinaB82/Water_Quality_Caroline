🏗️ Architecture du pipeline Data Engineering – Qualité de l’eau

🎯 Objectif

Mettre en place un pipeline de données moderne basé sur l’architecture médallion (Bronze, Silver, Gold) afin de traiter et analyser la qualité de l’eau en France.

🔁 Vue globale du pipeline
Sources (Hub’Eau / fichiers)
        ↓
     Bronze
        ↓
     Silver
        ↓
      Gold
        ↓
Analyse SQL (DuckDB)
        ↓
Insights métiers


🥉 Couche Bronze — Ingestion

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
data/bronze/
├── analyses/
├── parametres/
└── stations/


🥈 Couche Silver — Nettoyage & Transformation

🧼 Traitements appliqués
suppression des valeurs nulles critiques
suppression des doublons
sélection des colonnes utiles
renommage des colonnes
standardisation des formats


🔍 Enrichissement
création d’un indicateur de conformité globale
séparation des données métiers (mesures, stations, conformité)


📁 Sortie
data/silver/
├── mesures/
├── stations/
└── conformite/



🥇 Couche Gold — Modélisation & Analyse

📊 Modélisation
Dimensions
dim_communes
dim_temps
Tables de faits
fact_conformite


📈 KPIs produits
taux de conformité par commune
taux de conformité par département
nombre de prélèvements
nombre de non-conformités
évolution temporelle


📁 Sortie
data/gold/
├── dimensions/
├── facts/
└── kpis/



🧮 Couche Analyse — DuckDB (V1 locale)

🎯 Rôle

En l’absence d’un environnement Databricks, une couche d’analyse locale a été mise en place avec DuckDB.

⚙️ Fonctionnement
les fichiers Gold (CSV) sont chargés comme tables virtuelles
les requêtes SQL sont stockées dans le dossier sql/
un script Python (query_duckdb.py) exécute les requêtes


📊 Types d’analyses réalisées
comparaison 2024 vs 2025
identification des communes les plus non conformes
détection des dégradations et améliorations
calcul de KPI globaux par année


⚠️ Précautions analytiques
filtrage des communes avec peu de prélèvements
prise en compte des biais liés aux petits volumes de données


🔄 Évolution prévue

Cette couche sera remplacée par :

Databricks SQL
tables Delta Lake
🧠 Logique du pipeline
séparation claire des responsabilités (Bronze / Silver / Gold)
conservation des données sources (traçabilité)
transformation progressive des données
préparation pour analyse SQL
abstraction des données via une couche analytique
⚙️ Orchestration

Le pipeline est orchestré via un script principal :

main.py

Permettant de lancer :

ingestion (Bronze)
transformation (Silver)
modélisation (Gold)
🧪 Qualité des données

Une couche dédiée permet de :

vérifier les colonnes obligatoires
détecter les doublons
valider que les tables ne sont pas vides


🚀 Évolutions possibles
intégration avec l’API Hub’Eau
migration vers Databricks (Delta Lake)
orchestration avec Databricks Workflows
monitoring et alerting
dashboard (Power BI / Databricks SQL)


🧩 Conclusion

Cette architecture permet :

une gestion scalable des données
une analyse fiable de la qualité de l’eau
une base solide pour des projets data engineering en production