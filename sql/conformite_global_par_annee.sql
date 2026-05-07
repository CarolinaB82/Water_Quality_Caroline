SELECT t.annee,
       COUNT(*) AS total_analyses,
       SUM(CASE WHEN f.conformite_bacterio = 'C' THEN 1 ELSE 0 END) AS conformes,
       ROUND(100.0 * SUM(CASE WHEN f.conformite_bacterio = 'C' THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_conformite
FROM fact_analyses f
JOIN dim_temps t ON f.date_id = t.date_id
GROUP BY t.annee
ORDER BY t.annee;