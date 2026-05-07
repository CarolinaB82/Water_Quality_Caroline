# 📚 Data Dictionary — Water Quality Pipeline

## Bronze

### `bronze/analyses/analyses.csv` ou `bronze/analyses/` (Parquet)

Données brutes issues des fichiers `DIS_PLV`.

| Colonne source | Description |
|---|---|
| `referenceprel` | Identifiant du prélèvement |
| `cddept` | Code du département |
| `cdreseau` | Code du réseau d’eau |
| `inseecommuneprinc` | Code INSEE de la commune principale |
| `nomcommuneprinc` | Nom de la commune |
| `dateprel` | Date du prélèvement |
| `heureprel` | Heure du prélèvement |
| `conclusionprel` | Conclusion sanitaire du prélèvement |
| `plvconformitebacterio` | Conformité bactériologique |
| `plvconformitechimique` | Conformité chimique |
| `annee_fichier` | Année extraite du fichier source |

---

## Silver

### `silver/analyses/` (Parquet)

| Colonne | Description |
|---|---|
| `id_prelevement` | Identifiant unique du prélèvement |
| `code_departement` | Code du département |
| `code_reseau` | Code du réseau |
| `code_commune` | Code INSEE commune |
| `nom_commune` | Nom de la commune |
| `date_prelevement` | Date du prélèvement |
| `conclusion` | Conclusion sanitaire |
| `conformite_bacterio` | `C` = conforme, autre valeur = non conforme |
| `conformite_chimique` | `C` = conforme, autre valeur = non conforme |
| `annee_fichier` | Année du fichier source |

### `silver/parametres/` (Parquet)

| Colonne | Description |
|---|---|
| `id_prelevement` | Identifiant du prélèvement |
| `code_parametre` | Code du paramètre |
| `code_parametresiseeaux` | Code SISEEAUX |
| `libelle_parametre_maj` | Libellé paramètre (majuscule) |
| `libelle_parametre_min` | Libellé paramètre (minuscule) |
| `libelle_parametre_web` | Libellé web |
| `qualite_parametre` | Qualité |
| `valeur_traduite` | Valeur numérique |
| ... | ... |

### `silver/stations/` (Parquet)

| Colonne | Description |
|---|---|
| `stations_id` | Identifiant station |
| `code_commune` | Code INSEE commune |
| `nom_commune` | Nom de la commune |
| `nom_quartier` | Quartier |
| `code_reseau` | Code réseau |
| `nom_reseau` | Nom du réseau |
| `debut_alim` | Date début alimentation |

---

## Gold

### `gold/fact_analyses/` (Parquet)

| Colonne | Description |
|---|---|
| `fact_id` | Identifiant de la ligne de fait |
| `stations_id` | Identifiant station |
| `date_id` | Identifiant date |
| `parametre_id` | Identifiant paramètre |
| `valeur` | Valeur mesurée |
| ... | ... |

### `gold/dim_stations/`, `gold/dim_parametres/`, `gold/dim_temps/` (Parquet)

Dimensions pour l’analyse (voir scripts gold pour détails).

---

## API

- Les variables d’environnement attendues : `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_JOB_ID` (dans `.env`)
- Endpoints principaux : `/` (GET), `/deploy` (POST)

---

## Notes
- Les fichiers peuvent être au format CSV (pandas) ou Parquet (pyspark/databricks).
- Les noms de colonnes peuvent varier selon la pipeline utilisée (voir scripts pour détails).