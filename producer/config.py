import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_VEHICLE_POSITIONS = os.getenv("TOPIC_VEHICLE_POSITIONS", "gtfs.vehicle_positions")
TOPIC_TRIP_UPDATES      = os.getenv("TOPIC_TRIP_UPDATES",      "gtfs.trip_updates")
TOPIC_SERVICE_ALERTS    = os.getenv("TOPIC_SERVICE_ALERTS",    "gtfs.service_alerts")

GTFS_VEHICLE_URL = os.getenv(
    "GTFS_VEHICLE_URL",
    "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"
)
GTFS_TRIP_URL = os.getenv(
    "GTFS_TRIP_URL",
    "https://realtime.hsl.fi/realtime/trip-updates/v2/hsl"
)
GTFS_ALERTS_URL = os.getenv(
    "GTFS_ALERTS_URL",
    "https://realtime.hsl.fi/realtime/service-alerts/v2/hsl"
)

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
USER_AGENT = "GTFS-Pipeline/1.0"
