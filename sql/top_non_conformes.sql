SELECT
    commune,
    code_departement,

    MAX(CASE WHEN annee_fichier = 2024 THEN nb_non_conformes END) AS non_conf_2024,
    MAX(CASE WHEN annee_fichier = 2025 THEN nb_non_conformes END) AS non_conf_2025,

    MAX(CASE WHEN annee_fichier = 2025 THEN nb_non_conformes END)
    - MAX(CASE WHEN annee_fichier = 2024 THEN nb_non_conformes END) AS evolution

FROM gold_kpi_conformite_commune

GROUP BY commune, code_departement

HAVING 
    non_conf_2024 IS NOT NULL
    AND non_conf_2025 IS NOT NULL

    -- 🔥 filtre qualité des données
    AND MAX(CASE WHEN annee_fichier = 2024 THEN nb_prelevements END) > 50
    AND MAX(CASE WHEN annee_fichier = 2025 THEN nb_prelevements END) > 50

ORDER BY non_conf_2025 DESC
LIMIT 20;