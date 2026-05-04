# Water_Quality_Caroline# 💧 Water Quality Data Pipeline (France)

Projet de data engineering dédié à l’analyse de la qualité de l’eau potable en France, basé sur les données du portail Hub’Eau.

---

## Version V1 locale

Cette première version exécute le pipeline localement avec Pandas.
Les tables Gold sont générées au format CSV.
Les requêtes analytiques sont exécutées localement avec DuckDB en attendant l’intégration Databricks / Delta Lake.

## 🎯 Objectif

Construire un pipeline de données complet suivant l’architecture médallion (Bronze, Silver, Gold) afin de :

- ingérer des données environnementales brutes
- les nettoyer et les transformer
- produire des indicateurs analytiques (KPIs)
- analyser la conformité de l’eau par commune et par département

---

## 🧱 Architecture

### 🥉 Bronze (Raw → Structuré)

Ingestion des données depuis les fichiers sources :

- `DIS_PLV` → prélèvements / analyses
- `DIS_RESULT` → résultats des paramètres
- `DIS_COM_UDI` → informations sur les communes et réseaux

👉 Sortie :

data/bronze/
├── analyses/
├── parametres/
└── stations/

---

### 🥈 Silver (Nettoyage & Transformation)

Traitement des données :

- suppression des valeurs manquantes
- suppression des doublons
- standardisation des colonnes
- création d’un indicateur de conformité

👉 Sortie :
data/silver/
├── mesures/
├── stations/
└── conformite/


---

### 🥇 Gold (Modélisation & Analyse)

Structuration des données pour l’analyse :

- dimensions (communes, temps)
- table de faits (conformité)
- KPIs

👉 Sortie :
data/gold/
├── dimensions/
├── facts/
└── kpis/


---

## ⚙️ Technologies utilisées

- Python
- Pandas
- PySpark (prévu pour Databricks)
- SQL
- Git / GitHub

---

## 🚀 Lancer le projet

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Initialisation de la structure du projet

Pour créer automatiquement tous les dossiers et fichiers nécessaires à la première utilisation, lance :

```bash
python setup_project.py
```

Cela permet de générer l'arborescence du projet (data/, src/, docs/, etc.) et des fichiers de base.

---

## 🧱 Lancer le pipeline :

```bash
python main.py
```

---

🧮 Requêtes analytiques avec DuckDB

En l’absence d’un environnement Databricks, les requêtes SQL sont exécutées localement à l’aide de DuckDB.

DuckDB permet de requêter directement les fichiers CSV produits dans la couche Gold, sans nécessiter de base de données externe.

🔧 Fonctionnement
Les fichiers Gold (data/gold/) sont chargés comme des tables virtuelles (views)
Les requêtes SQL sont stockées dans le dossier sql/
Un script Python (query_duckdb.py) exécute automatiquement les requêtes
▶️ Lancer les requêtes
python query_duckdb.py

📊 Exemples d’analyses
Top des communes avec le plus de non-conformités
Comparatif 2024 vs 2025
Communes les plus dégradées / améliorées
KPI global par année



📊 Résultats Finaux 

Le pipeline produit :

taux de conformité par commune
taux de conformité par département
évolution temporelle de la qualité de l’eau
identification des zones les plus à risque
🧪 Qualité des données

Des contrôles ont été mis en place :

vérification des colonnes obligatoires
détection des doublons
validation des tables non vides


📁 Structure du projet
src/
├── ingestion/
├── transformation/
├── gold/
├── quality/
└── utils/

data/
├── raw/
├── bronze/
├── silver/
└── gold/

docs/
sql/
tests/

📌 Améliorations possibles
intégration API Hub’Eau en temps réel
utilisation de Databricks et Delta Lake
orchestration avec Databricks Workflows
visualisation avec Databricks SQL / Power BI