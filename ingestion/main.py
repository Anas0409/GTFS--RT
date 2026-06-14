import os
import threading
import logging
import producer
import consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("main")


def _run(name, target) -> None:
    try:
        target()
    except Exception:
        log.exception("Thread %s a planté — arrêt du conteneur", name)
        os._exit(1)


if __name__ == "__main__":
    log.info("Starting GTFS-RT ingestion pipeline")

    t_producer = threading.Thread(target=_run, args=("producer", producer.run), name="producer", daemon=True)
    t_consumer = threading.Thread(target=_run, args=("consumer", consumer.run), name="consumer", daemon=True)

    t_producer.start()
    t_consumer.start()

    t_producer.join()
    t_consumer.join()
