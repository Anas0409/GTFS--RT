"""
Boucle principale : fetch → parse → publish → sleep.
"""
import logging
import signal
import time
import config
from fetcher import fetch_vehicle_positions, fetch_trip_updates, fetch_service_alerts
from producer import create_producer, publish

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

running = True


def shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received, exiting...")
    running = False


def main():
    global running
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logger.info("GTFS Producer starting...")
    producer = create_producer()

    iteration = 0
    while running:
        iteration += 1
        logger.info(f"-- Iteration #{iteration} ---------------------")

        vp = fetch_vehicle_positions()
        if vp:
            n = publish(producer, config.TOPIC_VEHICLE_POSITIONS, vp, "full_vehicle_id")
            logger.info(f"  vehicle_positions : {n}/{len(vp)} sent")
        else:
            logger.info("  vehicle_positions : 0/0 sent")

        tu = fetch_trip_updates()
        if tu:
            n = publish(producer, config.TOPIC_TRIP_UPDATES, tu, "route_id")
            logger.info(f"  trip_updates      : {n}/{len(tu)} sent")

        sa = fetch_service_alerts()
        if sa:
            n = publish(producer, config.TOPIC_SERVICE_ALERTS, sa, None)
            logger.info(f"  service_alerts    : {n}/{len(sa)} sent")

        if running:
            logger.info(f"  sleeping {config.POLL_INTERVAL_SECONDS}s...")
            time.sleep(config.POLL_INTERVAL_SECONDS)

    producer.close()
    logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
