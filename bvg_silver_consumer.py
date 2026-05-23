import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_CONFIG_CONSUMER = {
    "bootstrap.servers": "localhost:9092",
    "group.id":          "bvg-silver-consumer",
    "auto.offset.reset": "earliest",
}

KAFKA_CONFIG_PRODUCER = {
    "bootstrap.servers": "localhost:9092",
}

BRONZE_TOPIC = "bvg-departures"
SILVER_TOPIC = "bvg-departures-silver"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "bvg_delays",
    "user":     "bvg",
    "password": "bvg",
}

# ── Deduplication cache ───────────────────────────────────────────────────────
# Keeps track of (trip_id, stop_id) pairs already processed in this session
# The UNIQUE constraint in the DB is the hard guarantee; this avoids unnecessary DB calls
seen = set()

# ── Validation ────────────────────────────────────────────────────────────────

def is_valid(msg: dict) -> bool:
    """Return True only if the message has all required fields and a real delay."""
    required = ["tripId", "line", "stopName", "stopId", "delay"]
    if any(msg.get(f) is None for f in required):
        return False
    if not isinstance(msg["delay"], (int, float)):
        return False
    return True

def clean(msg: dict) -> dict:
    """Normalise raw bronze message into a clean silver record."""
    delay_s   = float(msg["delay"])
    delay_min = round(delay_s / 60, 2)
    return {
        "tripId":      msg["tripId"],
        "line":        msg["line"].strip().upper(),
        "product":     msg.get("product"),
        "direction":   msg.get("direction"),
        "stopName":    msg["stopName"],
        "stopId":      msg["stopId"],
        "plannedWhen": msg.get("plannedWhen"),
        "actualWhen":  msg.get("when"),
        "delayS":      delay_s,
        "delayMin":    delay_min,
        "recordedAt":  datetime.now(timezone.utc).isoformat(),
    }

# ── PostgreSQL sink ───────────────────────────────────────────────────────────

def save_to_silver(conn, record: dict):
    """Insert a clean record into silver_departures. Skips on duplicate (trip_id, stop_id)."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO silver_departures
                    (trip_id, line, product, direction, stop_name, stop_id,
                     planned_when, actual_when, delay_s, delay_min)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trip_id, stop_id) DO NOTHING
            """, (
                record["tripId"],
                record["line"],
                record["product"],
                record["direction"],
                record["stopName"],
                record["stopId"],
                record["plannedWhen"],
                record["actualWhen"],
                record["delayS"],
                record["delayMin"],
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB insert failed: {e}")

# ── Delivery callback ─────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        logger.error(f"Silver delivery failed for key '{msg.key()}': {err}")
    else:
        logger.debug(f"Silver message delivered to {msg.topic()} [{msg.partition()}]")

# ── Main loop ─────────────────────────────────────────────────────────────────

def run_silver_consumer():
    consumer = Consumer(KAFKA_CONFIG_CONSUMER)
    consumer.subscribe([BRONZE_TOPIC])

    producer = Producer(KAFKA_CONFIG_PRODUCER)

    conn = psycopg2.connect(**DB_CONFIG)
    logger.info(f"Silver consumer started. {BRONZE_TOPIC} → clean → {SILVER_TOPIC} + DB")

    stats = {"received": 0, "skipped": 0, "duplicates": 0, "saved": 0}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            # ── Parse ─────────────────────────────────────────────────────────
            try:
                raw = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                continue

            stats["received"] += 1

            # ── Validate ──────────────────────────────────────────────────────
            if not is_valid(raw):
                stats["skipped"] += 1
                continue

            # ── Deduplicate ───────────────────────────────────────────────────
            dedup_key = (raw["tripId"], raw["stopId"])
            if dedup_key in seen:
                stats["duplicates"] += 1
                continue
            seen.add(dedup_key)

            # ── Clean ─────────────────────────────────────────────────────────
            record = clean(raw)

            # ── Save to PostgreSQL ────────────────────────────────────────────
            save_to_silver(conn, record)
            stats["saved"] += 1

            # ── Publish to silver topic ───────────────────────────────────────
            producer.produce(
                topic=SILVER_TOPIC,
                key=record["line"].encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            if stats["received"] % 20 == 0:
                logger.info(f"Stats: {stats}")

    except KeyboardInterrupt:
        logger.info("Silver consumer stopped.")
    finally:
        producer.flush()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    run_silver_consumer()