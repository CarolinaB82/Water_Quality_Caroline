@echo off
REM Script d'automatisation pour exécuter toute la chaîne pandas (bronze + silver)
cd /d %~dp0
python src\pandas\bronze\ingest_parametres.py
python src\pandas\bronze\ingest_stations.py
python src\pandas\bronze\ingest_analyses.py

