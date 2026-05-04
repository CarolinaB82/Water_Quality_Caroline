import duckdb
from pathlib import Path

con = duckdb.connect()

# Création de "fausses tables" DuckDB à partir de tes CSV Gold
con.execute("""
CREATE OR REPLACE VIEW gold_kpi_conformite_commune AS
SELECT *
FROM read_csv_auto('data/gold/kpis/kpi_conformite_commune.csv', delim=';');
""")

con.execute("""
CREATE OR REPLACE VIEW gold_fact_conformite AS
SELECT *
FROM read_csv_auto('data/gold/facts/fact_conformite.csv', delim=';');
""")


def run_sql_file(title, filepath):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

    query = Path(filepath).read_text(encoding="utf-8")
    result = con.sql(query)
    print(result)


run_sql_file(
    "KPI CONFORMITÉ COMMUNE",
    "sql/kpi_conformite_commune.sql"
)

run_sql_file(
    "TENDANCE PARAMÈTRES / NON-CONFORMITÉS",
    "sql/tendance_parametres.sql"
)

run_sql_file(
    "TOP NON-CONFORMES",
    "sql/top_non_conformes.sql"
)

run_sql_file(
    "COMPARATIF 2024 VS 2025",
    "sql/comparatif_2024_2025.sql"
)