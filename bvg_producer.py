import time
import json
import logging
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",  # update if running remotely
}

TOPIC = "bvg-departures"
POLL_INTERVAL_SECONDS = 30
BVG_API_BASE = "https://v6.bvg.transport.rest"

# The 4 chosen stops with their BVG stop IDs
STOPS = {
    "Gesundbrunnen":      "900007102",
    "Südkreuz":           "900058101",
    "Alexanderplatz":     "900100003",
    "Zoologischer Garten":"900023201",
}

# ── Kafka delivery callback ───────────────────────────────────────────────────

def delivery_report(err, msg):
    """Called by confluent-kafka once a message is delivered or fails."""
    if err:
        logger.error(f"Delivery failed for key '{msg.key()}': {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

# ── BVG API ───────────────────────────────────────────────────────────────────

def fetch_departures(stop_name: str, stop_id: str, duration: int = 10) -> list[dict]:
    """
    Fetch upcoming departures for a stop.
    duration = how many minutes ahead to look.
    """
    url = f"{BVG_API_BASE}/stops/{stop_id}/departures"
    params = {"duration": duration, "results": 20} #"results": 50

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        departures = data.get("departures", [])
        logger.info(f"Fetched {len(departures)} departures from {stop_name}")
        return departures
    except requests.RequestException as e:
        logger.error(f"Failed to fetch departures for {stop_name}: {e}")
        return []

# ── Message builder ───────────────────────────────────────────────────────────

def build_message(departure: dict, stop_name: str, stop_id: str) -> dict | None:
    """
    Extract the fields we care about from a raw BVG departure.
    Returns None if essential fields are missing.
    """
    line_info = departure.get("line")
    if not line_info:
        return None

    line_name = line_info.get("name")
    if not line_name:
        return None

    return {
        "tripId":      departure.get("tripId"),
        "line":        line_name,
        "product":     line_info.get("product"),     # subway, suburban, bus, tram ...
        "direction":   departure.get("direction"),
        "stopName":    stop_name,
        "stopId":      stop_id,
        "plannedWhen": departure.get("plannedWhen"), # ISO 8601 scheduled time
        "when":        departure.get("when"),         # ISO 8601 actual time
        "delay":       departure.get("delay"),        # seconds, can be None
        "fetchedAt":   datetime.now(timezone.utc).isoformat(),
    }

# ── Producer loop ─────────────────────────────────────────────────────────────

def run_producer():
    producer = Producer(KAFKA_CONFIG)
    logger.info(f"Producer started. Publishing to topic '{TOPIC}' every {POLL_INTERVAL_SECONDS}s.")

    try:
        while True:
            for stop_name, stop_id in STOPS.items():
                departures = fetch_departures(stop_name, stop_id)

                for departure in departures:
                    message = build_message(departure, stop_name, stop_id)
                    if message is None:
                        continue

                    # Key = line name → all events for the same line go to the same partition
                    # This makes per-line aggregation efficient in Kafka Streams (Step 3)
                    key = message["line"]

                    producer.produce(
                        topic=TOPIC,
                        key=key.encode("utf-8"),
                        value=json.dumps(message).encode("utf-8"),
                        callback=delivery_report,
                    )

                # Flush after each stop so messages aren't held in the internal buffer
                producer.poll(0)

            # Wait for all in-flight messages to be delivered before sleeping
            producer.flush()
            logger.info(f"Poll cycle complete. Sleeping {POLL_INTERVAL_SECONDS}s...\n")
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_producer()