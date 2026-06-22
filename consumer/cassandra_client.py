"""
Connexion à Cassandra et insertion des donnees.
1 message = 1 insert (pas de batch).
"""
import logging
import time
from cassandra.cluster import Cluster
from cassandra.policies import ExponentialReconnectionPolicy
import config

logger = logging.getLogger(__name__)

INSERT_VEHICLE = """
INSERT INTO vehicle_positions (
    operator_id, vehicle_id, full_vehicle_id,
    route_id, direction_id, start_date, start_time,
    latitude, longitude, bearing, speed, current_status,
    gtfs_timestamp, ingestion_time
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TRIP = """
INSERT INTO trip_updates (
    route_id, start_date, start_time, direction_id,
    avg_delay_seconds, num_stops,
    gtfs_timestamp, ingestion_time
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_ALERT = """
INSERT INTO service_alerts (
    ingestion_time, header, description, start_time, end_time
) VALUES (?, ?, ?, ?, ?)
"""


class CassandraClient:
    def __init__(self):
        self.session  = None
        self.stmt_vp  = None
        self.stmt_tu  = None
        self.stmt_sa  = None
        self._connect()

    def _connect(self, retries: int = 20):
        for attempt in range(1, retries + 1):
            try:
                cluster = Cluster(
                    [config.CASSANDRA_HOST],
                    port=config.CASSANDRA_PORT,
                    reconnection_policy=ExponentialReconnectionPolicy(1, 60),
                )
                self.session = cluster.connect(config.CASSANDRA_KEYSPACE)
                self.stmt_vp = self.session.prepare(INSERT_VEHICLE)
                self.stmt_tu = self.session.prepare(INSERT_TRIP)
                self.stmt_sa = self.session.prepare(INSERT_ALERT)
                logger.info(f"Cassandra connected ({config.CASSANDRA_HOST})")
                return
            except Exception as e:
                logger.warning(f"Cassandra not ready ({attempt}/{retries}): {e}")
                time.sleep(5)
        raise RuntimeError("Cannot connect to Cassandra")

    def insert_vehicle_position(self, msg: dict):
        self.session.execute(self.stmt_vp, (
            msg["operator_id"],
            msg["vehicle_id"],
            msg["full_vehicle_id"],
            msg["route_id"],
            msg["direction_id"],
            msg["start_date"],
            msg["start_time"],
            msg["latitude"],
            msg["longitude"],
            msg["bearing"],
            msg["speed"],
            msg["current_status"],
            msg["gtfs_timestamp"],
            msg["ingestion_time"],
        ))

    def insert_trip_update(self, msg: dict):
        self.session.execute(self.stmt_tu, (
            msg["route_id"],
            msg["start_date"],
            msg["start_time"],
            msg["direction_id"],
            msg["avg_delay_seconds"],
            msg["num_stops"],
            msg["gtfs_timestamp"],
            msg["ingestion_time"],
        ))

    def insert_service_alert(self, msg: dict):
        self.session.execute(self.stmt_sa, (
            msg["ingestion_time"],
            msg["header"],
            msg["description"],
            msg["start_time"],
            msg["end_time"],
        ))
