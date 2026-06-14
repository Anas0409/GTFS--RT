import os
import time
import json
import logging
import requests
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

GTFS_RT_URL = os.environ.get("GTFS_RT_URL")
if not GTFS_RT_URL:
    log.error("La variable d'environnement GTFS_RT_URL est obligatoire (voir .env.example)")
    raise SystemExit(1)

GTFS_RT_TRIPUPDATES_URL = os.environ.get("GTFS_RT_TRIPUPDATES_URL")
KAFKA_BOOTSTRAP         = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FETCH_INTERVAL          = int(os.environ.get("FETCH_INTERVAL", "30"))

TOPIC_VEHICLES = "gtfs_vehicle_positions"
TOPIC_TRIPS    = "gtfs_trip_updates"


def _delivery(err, msg):
    if err:
        log.error("Delivery failed [%s]: %s", msg.topic(), err)


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    resp = requests.get(url, timeout=15, headers={"Accept": "application/x-protobuf"})
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def publish(producer: Producer, feed: gtfs_realtime_pb2.FeedMessage) -> None:
    feed_ts = feed.header.timestamp
    n_vehicles = n_trips = 0

    for entity in feed.entity:
        if entity.is_deleted:
            continue

        if entity.HasField("vehicle"):
            v = entity.vehicle
            payload = {
                "entity_id":      entity.id,
                "vehicle_id":     v.vehicle.id or entity.id,
                "label":          v.vehicle.label,
                "trip_id":        v.trip.trip_id,
                "route_id":       v.trip.route_id,
                "direction_id":   v.trip.direction_id,
                "start_date":     v.trip.start_date,
                "latitude":       v.position.latitude,
                "longitude":      v.position.longitude,
                "bearing":        v.position.bearing,
                "speed":          v.position.speed,
                "current_status": v.current_status,
                "stop_sequence":  v.current_stop_sequence,
                "stop_id":        v.stop_id,
                "timestamp":      int(v.timestamp) if v.timestamp else feed_ts,
            }
            producer.produce(
                TOPIC_VEHICLES,
                key=payload["vehicle_id"],
                value=json.dumps(payload),
                callback=_delivery,
            )
            n_vehicles += 1

        if entity.HasField("trip_update"):
            tu = entity.trip_update
            stop_updates = []
            for stu in tu.stop_time_update:
                update = {
                    "stop_sequence": stu.stop_sequence,
                    "stop_id":       stu.stop_id,
                }
                if stu.HasField("arrival"):
                    update["arrival_delay"] = stu.arrival.delay
                    update["arrival_time"]  = stu.arrival.time
                if stu.HasField("departure"):
                    update["departure_delay"] = stu.departure.delay
                    update["departure_time"]  = stu.departure.time
                stop_updates.append(update)

            payload = {
                "entity_id":   entity.id,
                "trip_id":     tu.trip.trip_id,
                "route_id":    tu.trip.route_id,
                "vehicle_id":  tu.vehicle.id,
                "delay":       tu.delay,
                "timestamp":   int(tu.timestamp) if tu.timestamp else feed_ts,
                "stop_updates": stop_updates,
            }
            producer.produce(
                TOPIC_TRIPS,
                key=payload["trip_id"],
                value=json.dumps(payload),
                callback=_delivery,
            )
            n_trips += 1

    producer.flush()
    log.info("Published — vehicles: %d  trip_updates: %d", n_vehicles, n_trips)


def run() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    log.info("Producer ready. URL=%s  interval=%ds", GTFS_RT_URL, FETCH_INTERVAL)
    if GTFS_RT_TRIPUPDATES_URL:
        log.info("Trip updates URL=%s", GTFS_RT_TRIPUPDATES_URL)

    while True:
        try:
            feed = fetch_feed(GTFS_RT_URL)
            publish(producer, feed)

            if GTFS_RT_TRIPUPDATES_URL:
                tu_feed = fetch_feed(GTFS_RT_TRIPUPDATES_URL)
                publish(producer, tu_feed)
        except Exception as exc:
            log.error("Fetch/publish error: %s", exc)
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    run()
