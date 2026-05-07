import pandas as pd


def clean_text_columns(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    df = df.copy()

    if columns is None:
        columns = df.columns

    for column in columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
                .replace({
                    "": pd.NA,
                    "nan": pd.NA,
                    "None": pd.NA,
                    "NaT": pd.NA
                })
            )

    return df


def uppercase_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype("string").str.upper()

    return df


def titlecase_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype("string").str.title()

    return df


def clean_date_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    return df


def clean_numeric_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype("string")
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
            )
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def drop_missing_required(df: pd.DataFrame, columns) -> pd.DataFrame:
    existing_columns = [column for column in columns if column in df.columns]

    if existing_columns:
        df = df.dropna(subset=existing_columns)

    return df


def add_sequential_id(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df[id_column] = df.index + 1

    columns = [id_column] + [column for column in df.columns if column != id_column]
    return df[columns]