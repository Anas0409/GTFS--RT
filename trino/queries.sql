-- ============================================================
-- Requêtes analytiques GTFS-RT — Trino → Cassandra
-- Catalogue : cassandra | Schéma : gtfs
-- ============================================================

-- 1. Dernière position connue de chaque véhicule (aujourd'hui)
SELECT
    vehicle_id,
    label,
    route_id,
    trip_id,
    latitude,
    longitude,
    bearing,
    ROUND(speed * 3.6, 1)   AS speed_kmh,
    current_status,         -- 0=INCOMING_AT  1=STOPPED_AT  2=IN_TRANSIT_TO
    timestamp
FROM cassandra.gtfs.vehicle_positions
WHERE bucket = CAST(current_date AS VARCHAR)
QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY timestamp DESC) = 1
ORDER BY route_id, vehicle_id;


-- 2. Nombre de véhicules actifs par ligne (aujourd'hui)
SELECT
    route_id,
    COUNT(DISTINCT vehicle_id) AS nb_vehicules
FROM cassandra.gtfs.vehicle_positions
WHERE bucket = CAST(current_date AS VARCHAR)
  AND timestamp >= NOW() - INTERVAL '5' MINUTE
GROUP BY route_id
ORDER BY nb_vehicules DESC;


-- 3. Retard moyen par ligne (en secondes)
SELECT
    route_id,
    COUNT(*)                        AS nb_mesures,
    ROUND(AVG(delay), 0)            AS retard_moyen_sec,
    ROUND(AVG(delay) / 60.0, 1)     AS retard_moyen_min,
    MAX(delay)                      AS retard_max_sec
FROM cassandra.gtfs.trip_updates
WHERE timestamp >= NOW() - INTERVAL '1' HOUR
GROUP BY route_id
ORDER BY retard_moyen_sec DESC;


-- 4. Lignes avec retard moyen > 3 minutes
SELECT
    route_id,
    ROUND(AVG(delay) / 60.0, 1) AS retard_moyen_min
FROM cassandra.gtfs.trip_updates
WHERE timestamp >= NOW() - INTERVAL '1' HOUR
GROUP BY route_id
HAVING AVG(delay) > 180
ORDER BY retard_moyen_min DESC;


-- 5. Vitesse moyenne par ligne (km/h)
SELECT
    route_id,
    ROUND(AVG(speed) * 3.6, 1)  AS vitesse_moy_kmh,
    ROUND(MAX(speed) * 3.6, 1)  AS vitesse_max_kmh,
    COUNT(*)                     AS nb_mesures
FROM cassandra.gtfs.vehicle_positions
WHERE bucket = CAST(current_date AS VARCHAR)
  AND speed > 0
GROUP BY route_id
ORDER BY vitesse_moy_kmh DESC;


-- 6. Évolution du retard moyen par heure (dernières 12h)
SELECT
    DATE_TRUNC('hour', timestamp)   AS heure,
    route_id,
    ROUND(AVG(delay) / 60.0, 1)    AS retard_moyen_min
FROM cassandra.gtfs.trip_updates
WHERE timestamp >= NOW() - INTERVAL '12' HOUR
GROUP BY DATE_TRUNC('hour', timestamp), route_id
ORDER BY heure DESC, route_id;


-- 7. Distribution des retards (tranches de 1 min)
SELECT
    FLOOR(delay / 60) AS tranche_minute,
    COUNT(*)           AS nb_trajets
FROM cassandra.gtfs.trip_updates
WHERE timestamp >= NOW() - INTERVAL '1' HOUR
GROUP BY FLOOR(delay / 60)
ORDER BY tranche_minute;


-- 8. Véhicules à l'arrêt depuis plus de 10 minutes
SELECT
    vehicle_id,
    label,
    route_id,
    stop_id,
    latitude,
    longitude,
    timestamp,
    DATE_DIFF('minute', timestamp, NOW()) AS arret_depuis_min
FROM cassandra.gtfs.vehicle_positions
WHERE bucket = CAST(current_date AS VARCHAR)
  AND current_status = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY timestamp DESC) = 1
HAVING DATE_DIFF('minute', timestamp, NOW()) > 10
ORDER BY arret_depuis_min DESC;
