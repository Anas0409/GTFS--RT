import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_VEHICLE_POSITIONS  = os.getenv("TOPIC_VEHICLE_POSITIONS", "gtfs.vehicle_positions")
TOPIC_TRIP_UPDATES       = os.getenv("TOPIC_TRIP_UPDATES",      "gtfs.trip_updates")
TOPIC_SERVICE_ALERTS     = os.getenv("TOPIC_SERVICE_ALERTS",    "gtfs.service_alerts")
KAFKA_GROUP_ID           = "gtfs-cassandra-consumer"

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "gtfs")
