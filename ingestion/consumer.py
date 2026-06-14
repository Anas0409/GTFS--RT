import os
import json
import time
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CASSANDRA_HOST  = os.environ.get("CASSANDRA_HOST", "cassandra")
GROUP_ID        = "gtfs_consumer_group"

TOPIC_VEHICLES = "gtfs_vehicle_positions"
TOPIC_TRIPS    = "gtfs_trip_updates"

INSERT_VEHICLE = """
INSERT INTO gtfs.vehicle_positions (
    vehicle_id, bucket, timestamp,
    latitude, longitude, bearing, speed,
    trip_id, route_id, direction_id, start_date,
    current_status, stop_sequence, stop_id, label
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TRIP = """
INSERT INTO gtfs.trip_updates (
    trip_id, timestamp,
    route_id, vehicle_id, delay, stop_updates
) VALUES (?, ?, ?, ?, ?, ?)
"""


def _ts(unix: int) -> datetime:
    return datetime.fromtimestamp(unix, tz=timezone.utc)


def _day_bucket(unix: int) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d")


def connect_cassandra(host: str):
    for attempt in range(1, 11):
        try:
            cluster = Cluster(
                [host],
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            )
            session = cluster.connect()
            log.info("Cassandra connected")
            return session
        except Exception as exc:
            log.warning("Cassandra not ready (attempt %d/10): %s", attempt, exc)
            time.sleep(10)
    raise RuntimeError("Cannot connect to Cassandra after 10 attempts")


def process_vehicle(session, prepared, data: dict) -> None:
    ts   = _ts(data["timestamp"])
    day  = _day_bucket(data["timestamp"])
    session.execute(prepared, (
        data.get("vehicle_id", ""),
        day,
        ts,
        data.get("latitude"),
        data.get("longitude"),
        data.get("bearing"),
        data.get("speed"),
        data.get("trip_id", ""),
        data.get("route_id", ""),
        data.get("direction_id"),
        data.get("start_date", ""),
        data.get("current_status"),
        data.get("stop_sequence"),
        data.get("stop_id", ""),
        data.get("label", ""),
    ))


def process_trip(session, prepared, data: dict) -> None:
    ts = _ts(data["timestamp"])
    session.execute(prepared, (
        data.get("trip_id", ""),
        ts,
        data.get("route_id", ""),
        data.get("vehicle_id", ""),
        data.get("delay", 0),
        json.dumps(data.get("stop_updates", [])),
    ))


def run() -> None:
    session      = connect_cassandra(CASSANDRA_HOST)
    prep_vehicle = session.prepare(INSERT_VEHICLE)
    prep_trip    = session.prepare(INSERT_TRIP)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC_VEHICLES, TOPIC_TRIPS])
    log.info("Consumer ready — topics: %s, %s", TOPIC_VEHICLES, TOPIC_TRIPS)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("Kafka error: %s", msg.error())
            continue
        try:
            data = json.loads(msg.value())
            if msg.topic() == TOPIC_VEHICLES:
                process_vehicle(session, prep_vehicle, data)
            elif msg.topic() == TOPIC_TRIPS:
                process_trip(session, prep_trip, data)
        except Exception as exc:
            log.error("Processing error [%s]: %s — payload: %s", msg.topic(), exc, msg.value()[:200])


if __name__ == "__main__":
    run()
