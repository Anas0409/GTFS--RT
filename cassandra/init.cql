-- Keyspace
CREATE KEYSPACE IF NOT EXISTS gtfs
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE gtfs;

-- Positions véhicules
-- Partitionné par (vehicle_id, bucket jour) pour éviter les partitions infinies
CREATE TABLE IF NOT EXISTS vehicle_positions (
    vehicle_id     TEXT,
    bucket         TEXT,       -- 'YYYY-MM-DD'
    timestamp      TIMESTAMP,
    latitude       DOUBLE,
    longitude      DOUBLE,
    bearing        FLOAT,
    speed          FLOAT,
    trip_id        TEXT,
    route_id       TEXT,
    direction_id   INT,
    start_date     TEXT,
    current_status INT,
    stop_sequence  INT,
    stop_id        TEXT,
    label          TEXT,
    PRIMARY KEY ((vehicle_id, bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 86400;

-- Mises à jour de trajets
CREATE TABLE IF NOT EXISTS trip_updates (
    trip_id      TEXT,
    timestamp    TIMESTAMP,
    route_id     TEXT,
    vehicle_id   TEXT,
    delay        INT,
    stop_updates TEXT,    -- JSON list
    PRIMARY KEY (trip_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 86400;
