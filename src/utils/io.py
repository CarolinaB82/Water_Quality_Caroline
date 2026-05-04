import pandas as pd
from pathlib import Path


def read_csv(path, sep=";", dtype=str):
    return pd.read_csv(path, sep=sep, dtype=dtype)


def write_csv(df, path, sep=";"):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, sep=sep)