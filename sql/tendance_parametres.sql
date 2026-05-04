SELECT
    annee_fichier,
    SUM(nb_prelevements) AS total_prelevements,
    SUM(nb_conformes) AS total_conformes,
    SUM(nb_non_conformes) AS total_non_conformes,
    ROUND(SUM(nb_conformes) * 100.0 / SUM(nb_prelevements), 2) AS taux_global_conformite
FROM gold_kpi_conformite_commune
GROUP BY annee_fichier
ORDER BY annee_fichier;