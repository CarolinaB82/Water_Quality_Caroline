from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_RAW = BASE_DIR / "data" / "raw"
DATA_BRONZE = BASE_DIR / "data" / "bronze"
DATA_SILVER = BASE_DIR / "data" / "silver"
DATA_GOLD = BASE_DIR / "data" / "gold"