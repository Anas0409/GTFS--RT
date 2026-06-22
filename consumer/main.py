import logging
import signal
from consumer import create_consumer, run
from cassandra_client import CassandraClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
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

    db       = CassandraClient()
    consumer = create_consumer()
    try:
        run(consumer, db, lambda: running)
    finally:
        consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()
