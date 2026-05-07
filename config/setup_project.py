from pathlib import Path

PROJECT_STRUCTURE = [
    # Data folders
    "data/raw",
    "data/pandas/bronze/analyses",
    "data/pandas/bronze/parametres",
    "data/pandas/bronze/stations",
    "data/pandas/silver/analyses",
    "data/pandas/silver/parametres",
    "data/pandas/silver/stations",
    "data/pandas/gold/dim_parametres",
    "data/pandas/gold/dim_stations",
    "data/pandas/gold/dim_temps",
    "data/pandas/gold/fact_analyses",
    "data/pyspark/bronze/analyses",
    "data/pyspark/bronze/parametres",
    "data/pyspark/bronze/stations",
    "data/pyspark/silver/analyses",
    "data/pyspark/silver/parametres",
    "data/pyspark/silver/stations",
    "data/pyspark/gold",

    # Source code folders
    "src/pandas/bronze",
    "src/pandas/silver",
    "src/pandas/gold",
    "src/pandas/quality",
    "src/pandas/utils",
    "src/pyspark/bronze",
    "src/pyspark/silver",
    "src/pyspark/gold",
    "src/pyspark/quality",
    "src/pyspark/config",
    "src/pyspark/utils",
    "src/utils",

    # Other folders
    "sql",
    "docs",
    "tests",
    "config",
    "workflows"
]

FILES = {
    # Orchestration scripts
    "main.py":
        """from src.pandas.bronze.ingest_analyses import run_ingestion_analyses\nfrom src.pandas.bronze.ingest_parametres import run_ingestion_parametres\nfrom src.pandas.bronze.ingest_stations import run_ingestion_stations\n\nfrom src.pandas.silver.silver_analyses import run_silver_analyses\nfrom src.pandas.silver.silver_stations import run_silver_stations\nfrom src.pandas.silver.silver_parametres import run_silver_parametres\n\nfrom src.pandas.quality.data_quality_checks import run_data_quality_checks\n\nfrom src.pandas.gold.dim_parametres import run_dim_parametres\nfrom src.pandas.gold.dim_stations import run_dim_stations\nfrom src.pandas.gold.dim_temps import run_dim_temps\nfrom src.pandas.gold.fact_analyses import run_fact_analyses\n\nRUN_BRONZE = False\nRUN_SILVER = False\nRUN_QUALITY = True\nRUN_GOLD = True\n\nif __name__ == \"__main__\":\n    print(\"🚀 Lancement pipeline Water Quality\")\n\n    if RUN_BRONZE:\n        run_ingestion_analyses()\n        run_ingestion_parametres()\n        run_ingestion_stations()\n\n    if RUN_SILVER:\n        run_silver_stations()\n        run_silver_analyses()\n        run_silver_parametres()\n\n    if RUN_QUALITY:\n        run_data_quality_checks()\n\n    if RUN_GOLD:\n        run_dim_stations()\n        run_dim_parametres()\n        run_dim_temps()\n        run_fact_analyses()\n""",
    "src/pyspark/main_pyspark.py":
        """import subprocess\nimport sys\nimport os\n\nPROJECT_ROOT = os.getcwd()\nPYTHONPATH = PROJECT_ROOT\n\nenv = os.environ.copy()\nenv[\"PYTHONPATH\"] = PYTHONPATH\n\nprint(\"🚀 Lancement pipeline PySpark\")\n\nscripts = [\n    # Bronze\n    \"src/pyspark/bronze/bronze_analyses.py\",\n    \"src/pyspark/bronze/bronze_parametres.py\",\n    \"src/pyspark/bronze/bronze_stations.py\",\n    # Silver\n    \"src/pyspark/silver/silver_analyses.py\",\n    \"src/pyspark/silver/silver_parametres.py\",\n    \"src/pyspark/silver/silver_stations.py\",\n    # Data Quality\n    \"src/pyspark/quality/data_quality_checks.py\",\n    # Great Expectations\n    \"src/pyspark/quality/validate_silver_with_ge.py\",\n    # Gold\n    \"src/pyspark/gold/gold_dim_stations.py\",\n    \"src/pyspark/gold/gold_dim_parametres.py\",\n    \"src/pyspark/gold/gold_dim_temps.py\",\n    \"src/pyspark/gold/gold_fact_analyses.py\",\n]\n\nfor script in scripts:\n    print(f\"▶️ Exécution : {script}\")\n    subprocess.run([sys.executable, script], env=env)\n""",
    # Readme, requirements, gitignore
    "README.md": "# Water Quality Data Pipeline\n",
    "requirements.txt": "pandas\nrequests\npyspark\ndelta-spark\n",
    ".gitignore": "data/raw/\n__pycache__/\n*.pyc\n.env\n",
    # Example SQL and docs
    "sql/kpi_conformite_commune.sql": "",
    "sql/tendance_parametres.sql": "",
    "sql/top_non_conformes.sql": "",
    "sql/comparatif_2024_2025.sql": "",
    "docs/architecture.md": "",
    "docs/data_dictionary.md": "",
    "docs/rapport_final.md": "",
    "workflows/databricks_workflow.json": "",
}


def create_project_structure():
    for folder in PROJECT_STRUCTURE:
        Path(folder).mkdir(parents=True, exist_ok=True)

    for file_path, content in FILES.items():
        path = Path(file_path)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    print("✅ Structure du projet (pandas & pyspark) créée avec succès.")


if __name__ == "__main__":
    create_project_structure()