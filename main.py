from src.pandas.bronze.ingest_analyses import run_ingestion_analyses
from src.pandas.bronze.ingest_parametres import run_ingestion_parametres
from src.pandas.bronze.ingest_stations import run_ingestion_stations

from src.pandas.silver.silver_analyses import run_silver_analyses
from src.pandas.silver.silver_stations import run_silver_stations
from src.pandas.silver.silver_parametres import run_silver_parametres

from src.pandas.quality.data_quality_checks import run_data_quality_checks

from src.pandas.gold.dim_parametres import run_dim_parametres
from src.pandas.gold.dim_stations import run_dim_stations
from src.pandas.gold.dim_temps import run_dim_temps
from src.pandas.gold.fact_analyses import run_fact_analyses

RUN_BRONZE = False
RUN_SILVER = False
RUN_QUALITY = True
RUN_GOLD = True


if __name__ == "__main__":
    print("🚀 Lancement pipeline Water Quality")

    if RUN_BRONZE:
        run_ingestion_analyses()
        run_ingestion_parametres()
        run_ingestion_stations()

    if RUN_SILVER:
        run_silver_stations()
        run_silver_analyses()
        run_silver_parametres()

    if RUN_QUALITY:
        run_data_quality_checks()

    if RUN_GOLD:
        run_dim_stations()
        run_dim_parametres()
        run_dim_temps()
        run_fact_analyses()

    print("✅ Pipeline terminé")