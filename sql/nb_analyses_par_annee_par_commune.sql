SELECT t.annee, s.nom_commune, COUNT(*) AS nb_analyses
FROM fact_analyses f
JOIN dim_stations s ON f.stations_id = s.stations_id
JOIN dim_temps t ON f.date_id = t.date_id
GROUP BY t.annee, s.nom_commune
ORDER BY t.annee, nb_analyses DESC;