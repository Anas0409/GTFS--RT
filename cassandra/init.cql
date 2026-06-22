-- Keyspace
CREATE KEYSPACE IF NOT EXISTS gtfs
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE gtfs;

-- Table : positions des véhicules
-- Partition par route_id + jour → requêtes par ligne / période
-- trip_id volontairement absent (non fiable selon doc HSL)
CREATE TABLE IF NOT EXISTS vehicle_positions (
    route_id        text,
    start_date      text,
    ingestion_time  bigint,
    operator_id     text,
    vehicle_id      text,
    full_vehicle_id text,
    direction_id    int,
    start_time      text,
    latitude        double,
    longitude       double,
    bearing         float,
    speed           float,
    current_status  text,
    gtfs_timestamp  bigint,
    PRIMARY KEY ((route_id, start_date), ingestion_time, vehicle_id)
) WITH CLUSTERING ORDER BY (ingestion_time DESC);

-- Table : retards par trajet
CREATE TABLE IF NOT EXISTS trip_updates (
    route_id            text,
    start_date          text,
    ingestion_time      bigint,
    start_time          text,
    direction_id        int,
    avg_delay_seconds   double,
    num_stops           int,
    gtfs_timestamp      bigint,
    PRIMARY KEY ((route_id, start_date), ingestion_time)
) WITH CLUSTERING ORDER BY (ingestion_time DESC);

-- Table : alertes service
CREATE TABLE IF NOT EXISTS service_alerts (
    ingestion_time bigint,
    header text,
    description text,
    start_time bigint,
    end_time bigint,
    PRIMARY KEY (ingestion_time)
);
