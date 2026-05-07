@echo off
REM Script d'automatisation pour exécuter toute la chaîne PySpark (bronze + silver)
cd /d %~dp0
python src\pyspark\bronze\bronze_parametres.py
python src\pyspark\bronze\bronze_stations.py
python src\pyspark\bronze\bronze_analyses.py
python src\pyspark\silver\silver_parametres.py
python src\pyspark\silver\silver_stations.py
python src\pyspark\silver\silver_analyses.py
