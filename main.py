from src.ingestion.ingest_analyses import run_ingestion_analyses
from src.ingestion.ingest_parametres import run_ingestion_parametres
from src.ingestion.ingest_stations import run_ingestion_stations

from src.transformation.clean_mesures import run_clean_mesures
from src.transformation.clean_stations import run_clean_stations
from src.transformation.build_conformite import run_build_conformite

from src.gold.build_dimensions import run_build_dimensions
from src.gold.build_facts import run_build_facts
from src.gold.build_kpis import run_build_kpis


RUN_BRONZE = False
RUN_SILVER = True
RUN_GOLD = True


if __name__ == "__main__":
    print("🚀 Lancement pipeline Water Quality")

    if RUN_BRONZE:
        run_ingestion_analyses()
        run_ingestion_parametres()
        run_ingestion_stations()

    if RUN_SILVER:
        run_clean_mesures()
        run_clean_stations()
        run_build_conformite()

    if RUN_GOLD:
        run_build_dimensions()
        run_build_facts()
        run_build_kpis()

    print("✅ Pipeline terminé")