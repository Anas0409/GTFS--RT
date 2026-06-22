"""
Publie les messages dans les topics Kafka.
"""
import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import config

logger = logging.getLogger(__name__)


def create_producer(retries: int = 15) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",
                retries=3,
                compression_type="gzip",
            )
            logger.info(f"✅ Kafka connected: {config.KAFKA_BOOTSTRAP_SERVERS}")
            return p
        except NoBrokersAvailable:
            logger.warning(f"⏳ Kafka not ready ({attempt}/{retries}), waiting 5s...")
            time.sleep(5)
    raise RuntimeError("❌ Cannot connect to Kafka after retries")


def publish(producer: KafkaProducer, topic: str, messages: list[dict], key_field: str | None = None) -> int:
    sent = 0
    for msg in messages:
        key = msg.get(key_field) if key_field else None
        try:
            producer.send(topic, key=key, value=msg).get(timeout=10)
            sent += 1
        except Exception as e:
            logger.error(f"Send error on {topic}: {e}")
    return sent
