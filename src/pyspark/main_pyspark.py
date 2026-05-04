import subprocess
import sys

print("🚀 Lancement pipeline PySpark")

subprocess.run([sys.executable, "src/pyspark/bronze/bronze_plv.py"], check=True)
subprocess.run([sys.executable, "src/pyspark/silver/silver_clean.py"], check=True)
subprocess.run([sys.executable, "src/pyspark/gold/gold_kpi.py"], check=True)

print("✅ Pipeline PySpark terminé")