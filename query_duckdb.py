import duckdb
from pathlib import Path

con = duckdb.connect()

# Création de vues DuckDB à partir des CSV Gold (PySpark)
con.execute("""
CREATE OR REPLACE VIEW dim_stations AS
SELECT *
FROM read_csv_auto('data/pyspark/gold/dim_stations.csv', delim=';');
""")

con.execute("""
CREATE OR REPLACE VIEW dim_temps AS
SELECT *
FROM read_csv_auto('data/pyspark/gold/dim_temps.csv', delim=';');
""")

con.execute("""
CREATE OR REPLACE VIEW dim_parametres AS
SELECT *
FROM read_csv_auto('data/pyspark/gold/dim_parametres.csv', delim=';');
""")

con.execute("""
CREATE OR REPLACE VIEW fact_analyses AS
SELECT *
FROM read_csv_auto('data/pyspark/gold/fact_analyses.csv', delim=';');
""")


# Exemple d'exécution de requêtes SQL stockées dans le dossier sql/
def run_sql_file(title, filepath):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

    query = Path(filepath).read_text(encoding="utf-8")
    result = con.sql(query)
    print(result)

# Exemple d'exécution de requêtes SQL stockées dans le dossier sql/
run_sql_file("Nombre d’analyses par année et par commune", "sql/nb_analyses_par_annee_par_commune.sql")
