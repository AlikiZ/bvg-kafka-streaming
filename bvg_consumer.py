import json
import logging
import psycopg2
import psycopg2.extras
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id":          "bvg-delay-aggregator",
    "auto.offset.reset": "earliest",  # read from beginning if no offset stored
}

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "bvg_delays",
    "user":     "bvg",
    "password": "bvg",
}


TOPIC          = "bvg-departures"
WINDOW_MINUTES = 5      # rolling window size
SAVE_EVERY     = 15    # write aggregations to DB every N messages consumed
PRINT_EVERY    = 15     # print summary every N messages consumed

# ── Rolling window store ──────────────────────────────────────────────────────

# Each entry: {"delay": int, "timestamp": datetime}
# Keyed by line name or "line||stop"
by_line      = defaultdict(list)   # { "U8": [{delay, timestamp}, ...] }
by_line_stop = defaultdict(list)   # { "U8||Alexanderplatz": [{delay, timestamp}, ...] }

def evict_old_entries(store: defaultdict, window_minutes: int):
    """Remove entries older than the rolling window from all keys."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    for key in list(store.keys()):
        store[key] = [e for e in store[key] if e["timestamp"] >= cutoff]
        if not store[key]:
            del store[key]

def add_entry(store: defaultdict, key: str, delay_seconds: int):
    store[key].append({
        "delay":     delay_seconds,
        "timestamp": datetime.now(timezone.utc),
    })

def average_delay(store: defaultdict, key: str) -> float | None:
    entries = store.get(key, [])
    if not entries:
        return None
    return sum(e["delay"] for e in entries) / len(entries)

# ──PostgreSQL sink ───────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def save_aggregations(conn):
    """Write current rolling averages to PostgreSQL."""
    now = datetime.now(timezone.utc)

    try:
        with conn.cursor() as cur:

            # ── delay_by_line ─────────────────────────────────────────────────
            by_line_rows = []
            for line, entries in by_line.items():
                if entries:
                    by_line_rows.append((
                        now,
                        line,
                        average_delay(entries),
                        len(entries),
                    ))

            if by_line_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO delay_by_line (recorded_at, line, avg_delay_s, sample_count)
                    VALUES %s
                """, by_line_rows)

            # ── delay_by_line_stop ────────────────────────────────────────────
            by_line_stop_rows = []
            for key, entries in by_line_stop.items():
                if entries:
                    line, stop_name = key.split("||")
                    by_line_stop_rows.append((
                        now,
                        line,
                        stop_name,
                        average_delay(entries),
                        len(entries),
                    ))

            if by_line_stop_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO delay_by_line_stop (recorded_at, line, stop_name, avg_delay_s, sample_count)
                    VALUES %s
                """, by_line_stop_rows)

        conn.commit()
        logger.info(f"Saved {len(by_line_rows)} line rows and {len(by_line_stop_rows)} line+stop rows to DB.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save aggregations: {e}")


# ── Summary printer ───────────────────────────────────────────────────────────

def print_summary():
    print("\n" + "=" * 60)
    print(f"  Rolling {WINDOW_MINUTES}-min average delays — {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)

    print("\n📊 Per line (all stations combined):")
    if not by_line:
        print("  No data yet.")
    for line in sorted(by_line.keys()):
        avg = average_delay(by_line, line)
        count = len(by_line[line])
        if avg is not None:
            print(f"  {line:<6} → {avg/60:5.1f} min avg delay  ({count} samples)")

    print("\n📍 Per line per stop:")
    if not by_line_stop:
        print("  No data yet.")
    for key in sorted(by_line_stop.keys()):
        line, stop = key.split("||")
        avg = average_delay(by_line_stop, key)
        count = len(by_line_stop[key])
        if avg is not None:
            print(f"  {line:<6} @ {stop:<25} → {avg/60:5.1f} min avg delay  ({count} samples)")

    print()

# ── Consumer loop ─────────────────────────────────────────────────────────────

def run_consumer():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    logger.info(f"Consumer started. Subscribed to '{TOPIC}'.")

    conn = get_db_connection()
    logger.info("Connected to PostgreSQL.")

    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # wait up to 1s for a message

            if msg is None:
                continue  # no message in this poll, keep waiting

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"End of partition: {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            # ── Parse message ─────────────────────────────────────────────────
            try:
                value = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                continue

            delay = value.get("delay")
            line = value.get("line")
            stop_name = value.get("stopName")

            # Skip if essential fields are missing or no real-time delay data
            if delay is None or not line or not stop_name:
                continue

            # ── Update rolling windows ────────────────────────────────────────
            evict_old_entries(by_line, WINDOW_MINUTES)
            evict_old_entries(by_line_stop, WINDOW_MINUTES)

            add_entry(by_line, line, delay)
            add_entry(by_line_stop, f"{line}||{stop_name}", delay)

            message_count += 1

            # ── Print and save every N messages ───────────────────────────────
            if message_count % SAVE_EVERY == 0:
                print_summary()
                save_aggregations(conn)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        save_aggregations(conn)
        conn.close()
        consumer.close()


if __name__ == "__main__":
    run_consumer()