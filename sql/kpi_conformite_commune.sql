SELECT
    annee_fichier,
    commune,
    code_departement,
    nb_prelevements,
    nb_conformes,
    nb_non_conformes,
    taux_conformite
FROM gold_kpi_conformite_commune
WHERE nb_prelevements >= 50
ORDER BY annee_fichier, taux_conformite ASC
LIMIT 40;