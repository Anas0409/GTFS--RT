"""
Lit les messages Kafka et les persiste dans Cassandra.
"""
import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import config
from cassandra_client import CassandraClient

logger = logging.getLogger(__name__)


def create_consumer(retries: int = 15) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(
                config.TOPIC_VEHICLE_POSITIONS,
                config.TOPIC_TRIP_UPDATES,
                config.TOPIC_SERVICE_ALERTS,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info("✅ Kafka consumer ready")
            return c
        except NoBrokersAvailable:
            logger.warning(f"⏳ Kafka not ready ({attempt}/{retries}), waiting 5s...")
            time.sleep(5)
    raise RuntimeError("❌ Cannot connect to Kafka")


def run(consumer: KafkaConsumer, db: CassandraClient, is_running=None):
    logger.info("Listening to Kafka topics...")
    counts = {config.TOPIC_VEHICLE_POSITIONS: 0, config.TOPIC_TRIP_UPDATES: 0, config.TOPIC_SERVICE_ALERTS: 0}

    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            if is_running and not is_running():
                break
            continue

        for topic_partition, messages in records.items():
            for record in messages:
                topic = record.topic
                msg   = record.value

                try:
                    if topic == config.TOPIC_VEHICLE_POSITIONS:
                        db.insert_vehicle_position(msg)
                    elif topic == config.TOPIC_TRIP_UPDATES:
                        db.insert_trip_update(msg)
                    elif topic == config.TOPIC_SERVICE_ALERTS:
                        db.insert_service_alert(msg)

                    consumer.commit()
                    counts[topic] += 1

                    if sum(counts.values()) % 500 == 0:
                        logger.info(f"  Inserted: VP={counts[config.TOPIC_VEHICLE_POSITIONS]} | TU={counts[config.TOPIC_TRIP_UPDATES]} | SA={counts[config.TOPIC_SERVICE_ALERTS]}")

                except Exception as e:
                    logger.error(f"Error processing [{topic}]: {e} | msg={msg}")
