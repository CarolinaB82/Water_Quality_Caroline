SELECT
    commune,
    code_departement,

    MAX(CASE WHEN annee_fichier = 2024 THEN taux_conformite END) AS taux_2024,
    MAX(CASE WHEN annee_fichier = 2025 THEN taux_conformite END) AS taux_2025,

    ROUND(
        MAX(CASE WHEN annee_fichier = 2025 THEN taux_conformite END)
        - MAX(CASE WHEN annee_fichier = 2024 THEN taux_conformite END),
        2
    ) AS evolution_taux,

    MAX(CASE WHEN annee_fichier = 2024 THEN nb_non_conformes END) AS non_conformes_2024,
    MAX(CASE WHEN annee_fichier = 2025 THEN nb_non_conformes END) AS non_conformes_2025

FROM gold_kpi_conformite_commune
GROUP BY commune, code_departement
HAVING taux_2024 IS NOT NULL
   AND taux_2025 IS NOT NULL
ORDER BY evolution_taux ASC
LIMIT 30;