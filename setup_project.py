from pathlib import Path

PROJECT_STRUCTURE = [
    "data/raw",
    "data/bronze/stations",
    "data/bronze/analyses",
    "data/bronze/parametres",
    "data/silver/stations",
    "data/silver/mesures",
    "data/silver/conformite",
    "data/gold/dimensions",
    "data/gold/facts",
    "data/gold/kpis",

    "src/ingestion",
    "src/transformation",
    "src/gold",
    "src/quality",
    "src/utils",

    "sql",
    "docs",
    "tests",
    "config",
    "workflows"
]

FILES = {
    "main.py": "",
    "README.md": "# Water Quality Data Pipeline\n",
    "requirements.txt": "pandas\nrequests\npyspark\ndelta-spark\n",
    ".gitignore": "data/raw/\n__pycache__/\n*.pyc\n.env\n",

    "src/ingestion/ingest_stations.py": "",
    "src/ingestion/ingest_analyses.py": "",
    "src/ingestion/ingest_parametres.py": "",

    "src/transformation/clean_stations.py": "",
    "src/transformation/clean_mesures.py": "",
    "src/transformation/build_conformite.py": "",

    "src/gold/build_dimensions.py": "",
    "src/gold/build_facts.py": "",
    "src/gold/build_kpis.py": "",

    "src/quality/data_quality_checks.py": "",
    "src/utils/paths.py": "",

    "sql/kpi_conformite_commune.sql": "",
    "sql/tendance_parametres.sql": "",
    "sql/top_non_conformes.sql": "",

    "docs/architecture.md": "",
    "docs/data_dictionary.md": "",
    "docs/rapport_final.md": "",

    "workflows/databricks_workflow.json": ""
}


def create_project_structure():
    for folder in PROJECT_STRUCTURE:
        Path(folder).mkdir(parents=True, exist_ok=True)

    for file_path, content in FILES.items():
        path = Path(file_path)
        if not path.exists():
            path.write_text(content, encoding="utf-8")

    print("✅ Structure du projet créée avec succès.")


if __name__ == "__main__":
    create_project_structure()