# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("CREATE DATABASE IF NOT EXISTS water_quality")
spark.sql("USE water_quality")

# COMMAND ----------

BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable"

URL_COMMUNES_UDI = f"{BASE_URL}/communes_udi"
URL_RESULTATS_DIS = f"{BASE_URL}/resultats_dis"

DATE_DEBUT = "2024-01-01"
DATE_FIN = "2024-12-31"

LIST_CODE_DEPARTEMENT = "09,75"

PAGE_SIZE_COMMUNES = 10000
PAGE_SIZE_RESULTATS = 10000

# COMMAND ----------

# MAGIC %md
# MAGIC Fonction pour generer les mois 

# COMMAND ----------

def generer_periodes_mensuelles(date_debut, date_fin):
    periodes = []

    current = datetime.strptime(date_debut, "%Y-%m-%d")
    end = datetime.strptime(date_fin, "%Y-%m-%d")

    while current <= end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)

        month_end = builtins.min(
            next_month - timedelta(seconds=1),
            end.replace(hour=23, minute=59, second=59)
        )

        periodes.append((
            current.strftime("%Y-%m-%d 00:00:00"),
            month_end.strftime("%Y-%m-%d %H:%M:%S")
        ))

        current = next_month

    return periodes

periodes_mensuelles = generer_periodes_mensuelles(DATE_DEBUT, DATE_FIN)

# COMMAND ----------

# MAGIC %md
# MAGIC Fonction API → Spark → Delta

# COMMAND ----------

def api_to_delta(
    url,
    params_base,
    table_name,
    page_size,
    source_api,
    mode_first_write="overwrite",
    pause=0.1
):
    page = 1
    first_write = True

    while True:
        params = params_base.copy()
        params["page"] = page
        params["size"] = page_size

        response = requests.get(url, params=params, timeout=300)

        if response.status_code not in [200, 206]:
            raise Exception(
                f"Erreur API {response.status_code} page {page} : {response.text[:500]}"
            )

        data_page = response.json().get("data", [])

        if len(data_page) == 0:
            break

        df_page = spark.createDataFrame(
            pd.DataFrame(data_page).astype(str)
        )

        df_page = (
            df_page
            .withColumn("source_api", lit(source_api))
            .withColumn("page_api", lit(page))
            .withColumn("ingestion_timestamp", current_timestamp())
        )

        write_mode = mode_first_write if first_write else "append"

        df_page.write \
            .format("delta") \
            .mode(write_mode) \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        print(f"{table_name} - page {page} : {len(data_page)} lignes écrites")

        first_write = False

        if len(data_page) < page_size:
            break

        page += 1
        time.sleep(pause)

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze stations

# COMMAND ----------

api_to_delta(
    url=URL_COMMUNES_UDI,
    params_base={
        "annee": "2024"
    },
    table_name="bronze_stations",
    page_size=PAGE_SIZE_COMMUNES,
    source_api="communes_udi"
)

# COMMAND ----------

print("bronze_stations :", spark.table("bronze_stations").count())

# COMMAND ----------

display(spark.table("bronze_stations").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze analyses

# COMMAND ----------

# DBTITLE 1,Cell 6
def api_resultats_dis_to_delta(
    url,
    table_name,
    date_min_prelevement,
    date_max_prelevement,
    code_departement,
    page_size=1000,
    pause=0.1,
    mode="append",
    max_retries=5,
    retry_wait=10
):
    page = 1

    while True:
        params = {
            "date_min_prelevement": date_min_prelevement,
            "date_max_prelevement": date_max_prelevement,
            "code_departement": code_departement,
            "page": page,
            "size": page_size
        }

        # Appel API avec retry
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=300)

                if response.status_code in [200, 206]:
                    break

                if response.status_code in [429, 500, 502, 503, 504]:
                    print(f"Erreur {response.status_code} page {page}, tentative {attempt}/{max_retries}")
                    time.sleep(retry_wait * attempt)
                    continue

                raise Exception(
                    f"Erreur API {response.status_code} page {page} : {response.text[:500]}"
                )

            except requests.exceptions.ReadTimeout:
                print(f"Timeout page {page}, tentative {attempt}/{max_retries}")
                time.sleep(retry_wait * attempt)

        else:
            raise Exception(f"Échec API après {max_retries} tentatives page {page}")

        data_page = response.json().get("data", [])

        if len(data_page) == 0:
            break

        df_page = spark.createDataFrame(
            pd.DataFrame(data_page).astype(str)
        )

        bronze_page = (
            df_page
            .withColumn("source_api", lit("resultats_dis"))
            .withColumn("periode_debut", lit(date_min_prelevement))
            .withColumn("periode_fin", lit(date_max_prelevement))
            .withColumn("page_api", lit(page))
            .withColumn("ingestion_timestamp", current_timestamp())
        )

        bronze_page.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        print(f"{date_min_prelevement[:7]} page {page} : {len(data_page)} lignes écrites")

        page += 1
        mode = "append"

        if len(data_page) < page_size:
            break

        time.sleep(pause)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bronze_analyses")

for i, (start, end) in enumerate(periodes_mensuelles):
    print(f"Appel fonction période {i+1}/{len(periodes_mensuelles)} : {start} → {end}")

    api_resultats_dis_to_delta(
        url=URL_RESULTATS_DIS,
        table_name="bronze_analyses",
        date_min_prelevement=start,
        date_max_prelevement=end,
        code_departement=LIST_CODE_DEPARTEMENT,
        page_size=1000,
        mode="append"
    )

# COMMAND ----------

spark.table("bronze_analyses").count()

# COMMAND ----------

for col in spark.table("bronze_analyses").columns:
    print(col)

# COMMAND ----------

display(spark.table("bronze_analyses").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze paramètres

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bronze_parametres")

# COMMAND ----------

bronze_parametres = (
    spark.table("bronze_analyses")
    .select(
        "code_parametre",
        "code_parametre_se",
        "code_parametre_cas",
        "libelle_parametre",
        "libelle_parametre_maj",
        "libelle_parametre_web",
        "code_type_parametre",
        "limite_qualite_parametre",
        "reference_qualite_parametre"
    )
    .dropDuplicates()
    .withColumn("source_api", lit("resultats_dis"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

bronze_parametres.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_parametres")

# COMMAND ----------

display(spark.table("bronze_parametres").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver partie

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver_parametres : clef primaire

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("code_parametre")

silver_parametres = (
    spark.table("bronze_parametres")
    .withColumn("parametres_id", row_number().over(window_spec))
)

# COMMAND ----------

silver_parametres = silver_parametres.select(
    "parametres_id",
    *[c for c in silver_parametres.columns if c != "parametres_id"]
)

display(silver_parametres)

# COMMAND ----------

silver_parametres.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_parametres")

# COMMAND ----------

display(spark.table("silver_parametres").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_analyses : Création clé étrangères vers silver_parametres

# COMMAND ----------

# Charger les tables
bronze_analyses = spark.table("bronze_analyses")
silver_parametres = spark.table("silver_parametres")

# Colonnes utilisées pour retrouver le bon paramètre
colonnes_parametres = [
    "code_parametre",
    "code_parametre_se",
    "code_parametre_cas",
    "libelle_parametre",
    "libelle_parametre_maj",
    "libelle_parametre_web",
    "code_type_parametre",
    "limite_qualite_parametre",
    "reference_qualite_parametre"
]

# Table de correspondance paramètres
dim_parametres = (
    silver_parametres
    .select("parametres_id", *colonnes_parametres)
)

silver_analyses = (
    bronze_analyses
    .join(
        dim_parametres,
        on=colonnes_parametres,
        how="left"
    )
)

# COMMAND ----------

display(silver_analyses.limit(10))


# COMMAND ----------

from pyspark.sql.functions import col
silver_analyses.filter(col("parametres_id").isNull()).count()

# COMMAND ----------

silver_analyses.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_analyses")

# COMMAND ----------

display(silver_analyses.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_analyses : suppression des anciennes colonnes concernant les paramètres vu qu'on a la clé étrangère

# COMMAND ----------

colonnes_parametres = [
    "code_parametre",
    "code_parametre_se",
    "code_parametre_cas",
    "libelle_parametre",
    "libelle_parametre_maj",
    "libelle_parametre_web",
    "code_type_parametre",
    "limite_qualite_parametre",
    "reference_qualite_parametre"
]
silver_analyses = silver_analyses.drop(*colonnes_parametres)

# COMMAND ----------

display(silver_analyses.limit(10))

# COMMAND ----------

silver_analyses.printSchema()
display(silver_analyses.limit(10))

# COMMAND ----------

silver_analyses.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_analyses")

# COMMAND ----------

display(spark.table("silver_analyses").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_analyses : création de la colonne code_reseau depuis la colonne reseaux

# COMMAND ----------

display(spark.table("silver_analyses").limit(20))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema_reseaux = ArrayType(
    StructType([
        StructField("code", StringType(), True),
        StructField("nom", StringType(), True)
    ])
)

silver_analyses = (
    silver_analyses
    # convertir string → JSON valide
    .withColumn("reseaux_json", from_json(regexp_replace(col("reseaux"), "'", '"'), schema_reseaux))
    
    # récupérer le premier élément
    .withColumn("code_reseau", col("reseaux_json")[0]["code"])
)

silver_analyses = silver_analyses.drop("reseaux_json")

# COMMAND ----------

display(silver_analyses.select("reseaux", "code_reseau").limit(20))

# COMMAND ----------

silver_analyses.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_analyses")

# COMMAND ----------

display(spark.table("silver_analyses").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_stations : copie simple de bronze_station

# COMMAND ----------

silver_stations = spark.table("bronze_stations")
silver_stations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_stations")

# COMMAND ----------

display(spark.table("silver_stations").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver : nettoyage des tables (données)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nettoyage silver_parametres

# COMMAND ----------

for col in spark.table("silver_parametres").columns:
    print(col)

# COMMAND ----------

from pyspark.sql.functions import col
silver_parametres = (
    silver_parametres
    .withColumn("code_parametre", trim(col("code_parametre")))
    .withColumn("code_parametre_se", trim(col("code_parametre_se")))
    .withColumn("code_parametre_cas", trim(col("code_parametre_cas")))
    .withColumn("libelle_parametre", initcap(trim(col("libelle_parametre"))))
    .withColumn("libelle_parametre_maj", upper(trim(col("libelle_parametre_maj"))))
    .withColumn("libelle_parametre_web", trim(col("libelle_parametre_web")))
    .withColumn("code_type_parametre", trim(col("code_type_parametre")))
)

silver_parametres = (
    silver_parametres
    .withColumn(
        "limite_qualite_parametre",
        regexp_replace(col("limite_qualite_parametre"), ",", ".")
    )
    .withColumn(
        "reference_qualite_parametre",
        regexp_replace(col("reference_qualite_parametre"), ",", ".")
    )
)


# COMMAND ----------

display(silver_parametres.limit(10))  

# COMMAND ----------

silver_parametres.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_parametres")

# COMMAND ----------

display(spark.table("silver_parametres").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nettoyage silver_stations

# COMMAND ----------

for col in spark.table("silver_stations").columns:
    print(col)

# COMMAND ----------

display(spark.table("silver_stations").limit(20))

# COMMAND ----------

from pyspark.sql.functions import *

silver_stations = spark.table("silver_stations")

silver_stations = (
    silver_stations
    # Nettoyage des textes
    .withColumn("code_commune", trim(col("code_commune")))
    .withColumn("nom_commune", trim(col("nom_commune")))
    .withColumn("nom_quartier", trim(col("nom_quartier")))
    .withColumn("code_reseau", trim(col("code_reseau")))
    .withColumn("nom_reseau", trim(col("nom_reseau")))
    
    # Typage
    .withColumn("annee", col("annee").cast("int"))
    .withColumn("debut_alim", to_date(col("debut_alim")))
    
    # Valeurs manquantes
    .filter(col("code_commune").isNotNull())
    .filter(col("code_reseau").isNotNull())
    
    # Doublons
    .dropDuplicates(["code_commune", "code_reseau", "annee"])
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_stations = Window.orderBy("code_commune", "code_reseau", "annee")

silver_stations = (
    silver_stations
    .withColumn("stations_id", row_number().over(window_stations))
)

silver_stations = silver_stations.select(
    "stations_id",
    *[c for c in silver_stations.columns if c != "stations_id"]
)

# COMMAND ----------

display(silver_stations.limit(10)) 

# COMMAND ----------

silver_stations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_stations")

# COMMAND ----------

display(spark.table("silver_stations").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nettoyage de silver_analyses

# COMMAND ----------

for col in spark.table("silver_analyses").columns:
    print(col)


# COMMAND ----------

display(spark.table("silver_analyses").limit(20))

# COMMAND ----------

from pyspark.sql.functions import *

silver_analyses = spark.table("silver_analyses")

silver_analyses = (
    silver_analyses
    # Nettoyage textes
    .withColumn("code_departement", trim(col("code_departement")))
    .withColumn("nom_departement", trim(col("nom_departement")))
    .withColumn("code_prelevement", trim(col("code_prelevement")))
    .withColumn("code_lieu_analyse", trim(col("code_lieu_analyse")))
    .withColumn("resultat_alphanumerique", trim(col("resultat_alphanumerique")))
    .withColumn("libelle_unite", trim(col("libelle_unite")))
    .withColumn("code_unite", trim(col("code_unite")))
    .withColumn("code_commune", trim(col("code_commune")))
    .withColumn("nom_commune", trim(col("nom_commune")))
    .withColumn("nom_uge", trim(col("nom_uge")))
    .withColumn("nom_distributeur", trim(col("nom_distributeur")))
    .withColumn("nom_moa", trim(col("nom_moa")))
    .withColumn("conclusion_conformite_prelevement", trim(col("conclusion_conformite_prelevement")))
    .withColumn("reference_analyse", trim(col("reference_analyse")))
    .withColumn("code_installation_amont", trim(col("code_installation_amont")))
    .withColumn("nom_installation_amont", trim(col("nom_installation_amont")))
    .withColumn("code_reseau", trim(col("code_reseau")))

    # Typage dates
    .withColumn("date_prelevement", to_timestamp(col("date_prelevement")))
    .withColumn("periode_debut", to_timestamp(col("periode_debut")))
    .withColumn("periode_fin", to_timestamp(col("periode_fin")))

    # Typage numérique
    .withColumn("resultat_numerique", regexp_replace(col("resultat_numerique"), ",", ".").cast("double"))
    .withColumn("resultat_alphanumerique", regexp_replace(col("resultat_alphanumerique"), ",", "."))
    .withColumn("parametres_id", col("parametres_id").cast("int"))
)


# COMMAND ----------

silver_analyses = (
    silver_analyses
    .filter(col("code_prelevement").isNotNull())
    .filter(col("reference_analyse").isNotNull())
    .filter(col("date_prelevement").isNotNull())
    .filter(col("parametres_id").isNotNull())
)

# COMMAND ----------

display(silver_analyses.limit(10)) 

# COMMAND ----------

silver_analyses.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_analyses")

# COMMAND ----------

display(spark.table("silver_analyses").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Partie

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création de dim_parametres

# COMMAND ----------

silver_parametres = spark.table("silver_parametres")
silver_parametres = silver_parametres.drop("source_api", "ingestion_timestamp")



# COMMAND ----------

display(silver_parametres.limit(10)) 

# COMMAND ----------

silver_parametres.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_parametres")

# COMMAND ----------

display(spark.table("dim_parametres").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création dim_stations

# COMMAND ----------

silver_stations = spark.table("silver_stations")
silver_stations = silver_stations.drop("source_api", "ingestion_timestamp", "page_api")

# COMMAND ----------

display(silver_stations.limit(10)) 

# COMMAND ----------

silver_stations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_stations")

# COMMAND ----------

display(spark.table("dim_stations").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création dim_temps

# COMMAND ----------

from pyspark.sql.functions import *

dim_temps = (
    spark.table("silver_analyses")
    .select("date_prelevement")
    .dropDuplicates()
    .withColumn("annee", year(col("date_prelevement")))
    .withColumn("mois", month(col("date_prelevement")))
    .withColumn("jour", dayofmonth(col("date_prelevement")))
    .withColumn("trimestre", quarter(col("date_prelevement")))
)

# COMMAND ----------

display(dim_temps.limit(10)) 

# COMMAND ----------

dim_temps.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_temps")

# COMMAND ----------

display(spark.table("dim_temps").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création fact_analyses

# COMMAND ----------

from pyspark.sql.functions import col, when

silver_analyses = spark.table("silver_analyses")
silver_analyses = (
    silver_analyses
    .withColumn(
        "statut_conformite",
        when(
            (col("conformite_limites_bact_prelevement") == "N") |
            (col("conformite_limites_pc_prelevement") == "N") |
            (col("conformite_references_bact_prelevement") == "N") |
            (col("conformite_references_pc_prelevement") == "N"),
            "non_conforme"
        )
        .when(
            (col("conformite_limites_bact_prelevement") == "C") |
            (col("conformite_limites_pc_prelevement") == "C") |
            (col("conformite_references_bact_prelevement") == "C") |
            (col("conformite_references_pc_prelevement") == "C"),
            "conforme"
        )
        .otherwise("non_evalue")
    )
)

# COMMAND ----------

display(silver_analyses.limit(10)) 

# COMMAND ----------

display(spark.table("silver_analyses").limit(20))

# COMMAND ----------


fact_analyses = (
    silver_analyses
    .select(
        "code_prelevement",
        "date_prelevement",
        "code_commune",
        "code_reseau",
        "parametres_id",
        "resultat_numerique",
        "statut_conformite"
    )
)

# COMMAND ----------

display(fact_analyses.limit(10)) 

# COMMAND ----------

fact_analyses.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_analyses")

# COMMAND ----------

display(spark.table("fact_analyses").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création clé primaire dans dim_temps

# COMMAND ----------

dim_temps = dim_temps.withColumnRenamed("date_prelevement", "date")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window = Window.orderBy("date")

dim_temps = dim_temps.withColumn("date_id", row_number().over(window))

# COMMAND ----------

display(dim_temps.limit(20))

# COMMAND ----------

dim_temps.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_temps")

# COMMAND ----------

display(spark.table("dim_temps").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création de la clé étrangère date dans fact_analyse et suppression de date_prelevement

# COMMAND ----------

fact_analyses = spark.table("fact_analyses")
dim_temps = spark.table("dim_temps")

from pyspark.sql.functions import to_date, col

fact_analyses = fact_analyses.withColumn(
    "date_join",
    to_date(col("date_prelevement"))
)

dim_temps = dim_temps.withColumn(
    "date_join",
    to_date(col("date"))
)

fact_analyses = (
    fact_analyses
    .join(
        dim_temps.select("date_join", "date_id"),
        on="date_join",
        how="left"
    )
)

# COMMAND ----------

display(fact_analyses.limit(500))

# COMMAND ----------

fact_analyses = fact_analyses.drop("date_join", "date_prelevement")

# COMMAND ----------

display(fact_analyses.limit(50))

# COMMAND ----------

fact_analyses.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_analyses")

# COMMAND ----------

display(spark.table("fact_analyses").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Vérifications dim et fact finaux

# COMMAND ----------

display(spark.table("dim_stations").limit(10))
display(spark.table("dim_parametres").limit(10))
display(spark.table("dim_temps").limit(10))
display(spark.table("fact_analyses").limit(10))