import pandas as pd
import os

from src.pandas.utils.cleaning import (
    clean_text_columns,
    uppercase_columns,
    titlecase_columns,
    clean_numeric_columns,
    drop_missing_required,
    add_sequential_id
)


def run_silver_parametres():
    print("🧼 Création et nettoyage silver_parametres")

    input_path = "data/raw/DIS_RESULT_2024.txt"
    output_path = "data/pandas/silver/parametres/parametres.csv"

    os.makedirs("data/pandas/silver/parametres", exist_ok=True)

    df = pd.read_csv(input_path, sep=",", dtype=str)

    df = df.rename(columns={
        "cddept": "code_departement",
        "referenceprel": "id_prelevement",
        "cdparametresiseeaux": "code_parametresiseeaux",
        "cdparametre": "code_parametre",
        "libmajparametre": "libelle_parametre_maj",
        "libminparametre": "libelle_parametre_min",
        "libwebparametre": "libelle_parametre_web",
        "qualitparam": "qualite_parametre",
        "insituana": "insitu_ana",
        "rqana": "rq_ana",
        "cdunitereferencesiseeaux": "code_unite_ref_siseeaux",
        "cdunitereference": "code_unite_ref",
        "limitequal": "limite_qualite_parametre",
        "refqual": "reference_qualite_parametre",
        "valtraduite": "valeur_traduite",
        "casparam": "cas_param",
        "referenceanl": "reference_analyse"
    })

    df = clean_text_columns(df)

    df = uppercase_columns(df, [
        "code_departement",
        "id_prelevement",
        "code_parametre",
        "code_parametresiseeaux",
        "code_unite_ref",
        "code_unite_ref_siseeaux",
        "reference_analyse"
    ])

    df = titlecase_columns(df, [
        "libelle_parametre_min",
        "libelle_parametre_web"
    ])

    df = clean_numeric_columns(df, [
        "limite_qualite_parametre",
        "reference_qualite_parametre",
        "valeur_traduite"
    ])

    df = drop_missing_required(df, [
        "id_prelevement",
        "code_parametre",
        "reference_analyse"
    ])

    df = df.drop_duplicates()
    df = add_sequential_id(df, "parametres_id")

    df.to_csv(output_path, index=False, sep=";")

    print(f"✅ Silver parametres créé : {output_path}")
    print("Shape :", df.shape)