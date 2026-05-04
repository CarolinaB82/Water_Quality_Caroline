# 📚 Data Dictionary — Water Quality Pipeline

## Bronze

### `bronze/analyses/analyses.csv`

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

### `silver/mesures/mesures.csv`

Table nettoyée des prélèvements.

| Colonne | Description |
|---|---|
| `id_prelevement` | Identifiant unique du prélèvement |
| `code_departement` | Code du département |
| `code_reseau` | Code du réseau |
| `code_commune` | Code INSEE commune |
| `commune` | Nom de la commune |
| `date_prelevement` | Date du prélèvement |
| `conclusion` | Conclusion sanitaire |
| `conformite_bacterio` | `C` = conforme, autre valeur = non conforme |
| `conformite_chimique` | `C` = conforme, autre valeur = non conforme |
| `conformite_globale` | Conforme / Non conforme |
| `annee_fichier` | Année du fichier source |

### `silver/conformite/conformite.csv`

Table dédiée aux indicateurs de conformité.

| Colonne | Description |
|---|---|
| `id_prelevement` | Identifiant du prélèvement |
| `code_departement` | Code département |
| `code_commune` | Code INSEE commune |
| `commune` | Nom de commune |
| `date_prelevement` | Date du prélèvement |
| `is_conforme` | 1 si conforme, 0 sinon |
| `is_non_conforme` | 1 si non conforme, 0 sinon |

---

## Gold

### `gold/dimensions/dim_communes.csv`

| Colonne | Description |
|---|---|
| `code_departement` | Code département |
| `code_commune` | Code INSEE commune |
| `commune` | Nom commune |

### `gold/dimensions/dim_temps.csv`

| Colonne | Description |
|---|---|
| `date_prelevement` | Date du prélèvement |
| `annee_fichier` | Année |
| `mois` | Mois du prélèvement |

### `gold/facts/fact_conformite.csv`

| Colonne | Description |
|---|---|
| `id_prelevement` | Identifiant prélèvement |
| `code_departement` | Code département |
| `code_commune` | Code commune |
| `commune` | Nom commune |
| `date_prelevement` | Date |
| `is_conforme` | Indicateur binaire de conformité |
| `is_non_conforme` | Indicateur binaire de non-conformité |
| `annee_fichier` | Année |

### `gold/kpis/kpi_conformite_commune.csv`

| Colonne | Description |
|---|---|
| `code_departement` | Code département |
| `code_commune` | Code commune |
| `commune` | Nom commune |
| `annee_fichier` | Année |
| `nb_prelevements` | Nombre total de prélèvements |
| `nb_conformes` | Nombre de prélèvements conformes |
| `nb_non_conformes` | Nombre de prélèvements non conformes |
| `taux_conformite` | Pourcentage de conformité |

---

## Codes de conformité

| Code | Signification |
|---|---|
| `C` | Conforme |
| `N` | Non conforme |
| vide / null | Information manquante |