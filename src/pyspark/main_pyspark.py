import subprocess
import sys
import os


PROJECT_ROOT = os.getcwd()
PYTHONPATH = PROJECT_ROOT

env = os.environ.copy()
env["PYTHONPATH"] = PYTHONPATH

print("🚀 Lancement pipeline PySpark")

scripts = [
    # Bronze
    "src/pyspark/bronze/bronze_analyses.py",
    "src/pyspark/bronze/bronze_parametres.py",
    "src/pyspark/bronze/bronze_stations.py",

    # Silver
    "src/pyspark/silver/silver_analyses.py",
    "src/pyspark/silver/silver_parametres.py",
    "src/pyspark/silver/silver_stations.py",

    # Data Quality
    "src/pyspark/quality/data_quality_checks.py",

    #Great Expectations
    "src/pyspark/quality/validate_silver_with_ge.py",

    # Gold
    "src/pyspark/gold/gold_dim_stations.py",
    "src/pyspark/gold/gold_dim_parametres.py",
    "src/pyspark/gold/gold_dim_temps.py",
    "src/pyspark/gold/gold_fact_analyses.py",
]

for script in scripts:
    print(f"▶️ Exécution : {script}")
    subprocess.run(
        [sys.executable, script],
        check=True,
        env=env
    )

print("✅ Pipeline PySpark terminé")