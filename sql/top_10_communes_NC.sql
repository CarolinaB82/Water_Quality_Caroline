SELECT s.nom_commune, COUNT(*) AS nb_non_conformes
FROM fact_analyses f
JOIN dim_stations s ON f.stations_id = s.stations_id
WHERE f.conformite_bacterio = 'N'
GROUP BY s.nom_commune
ORDER BY nb_non_conformes DESC
LIMIT 10;