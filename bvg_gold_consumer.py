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
    "group.id":          "bvg-gold-consumer",
    "auto.offset.reset": "earliest",
}

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "bvg_delays",
    "user":     "bvg",
    "password": "bvg",
}

SILVER_TOPIC   = "bvg-departures-silver"
WINDOW_MINUTES = 5
SAVE_EVERY     = 10

# ── Rolling window store ──────────────────────────────────────────────────────

by_line      = defaultdict(list)  # { "U8": [{delay_s, timestamp}, ...] }
by_line_stop = defaultdict(list)  # { "U8||Alexanderplatz": [{delay_s, timestamp}, ...] }

def evict_old_entries(store: defaultdict):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=WINDOW_MINUTES)
    for key in list(store.keys()):
        store[key] = [e for e in store[key] if e["timestamp"] >= cutoff]
        if not store[key]:
            del store[key]

def add_entry(store: defaultdict, key: str, delay_s: float):
    store[key].append({"delay_s": delay_s, "timestamp": datetime.now(timezone.utc)})

def avg(entries: list) -> float:
    return sum(e["delay_s"] for e in entries) / len(entries)

# ── PostgreSQL sink ───────────────────────────────────────────────────────────

def save_gold(conn):
    """Write current rolling averages to gold tables."""
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:

            # delay_by_line
            line_rows = [
                (now, line, avg(entries), round(avg(entries) / 60, 2), len(entries))
                for line, entries in by_line.items() if entries
            ]
            if line_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO delay_by_line (recorded_at, line, avg_delay_s, avg_delay_min, sample_count)
                    VALUES %s
                """, line_rows)

            # delay_by_line_stop
            line_stop_rows = []
            for key, entries in by_line_stop.items():
                if entries:
                    line, stop_name = key.split("||")
                    line_stop_rows.append((
                        now, line, stop_name,
                        avg(entries), round(avg(entries) / 60, 2), len(entries)
                    ))
            if line_stop_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO delay_by_line_stop (recorded_at, line, stop_name, avg_delay_s, avg_delay_min, sample_count)
                    VALUES %s
                """, line_stop_rows)

        conn.commit()
        logger.info(f"Gold saved: {len(line_rows)} line rows, {len(line_stop_rows)} line+stop rows.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save gold aggregations: {e}")

# ── Summary printer ───────────────────────────────────────────────────────────

def print_summary():
    print("\n" + "=" * 60)
    print(f"  🥇 Gold — Rolling {WINDOW_MINUTES}-min averages  {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)

    print("\n📊 Per line (all stations):")
    if not by_line:
        print("  No data yet.")
    for line in sorted(by_line.keys()):
        entries = by_line[line]
        if entries:
            print(f"  {line:<6} → {avg(entries)/60:5.1f} min  ({len(entries)} samples)")

    print("\n📍 Per line per stop:")
    if not by_line_stop:
        print("  No data yet.")
    for key in sorted(by_line_stop.keys()):
        line, stop = key.split("||")
        entries = by_line_stop[key]
        if entries:
            print(f"  {line:<6} @ {stop:<25} → {avg(entries)/60:5.1f} min  ({len(entries)} samples)")
    print()

# ── Main loop ─────────────────────────────────────────────────────────────────

def run_gold_consumer():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([SILVER_TOPIC])

    conn = psycopg2.connect(**DB_CONFIG)
    logger.info(f"Gold consumer started. Subscribed to '{SILVER_TOPIC}'.")

    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                continue

            # Silver records are already clean — safe to use directly
            delay_s   = record.get("delayS")
            line      = record.get("line")
            stop_name = record.get("stopName")

            if delay_s is None or not line or not stop_name:
                continue

            evict_old_entries(by_line)
            evict_old_entries(by_line_stop)

            add_entry(by_line,      line,                   delay_s)
            add_entry(by_line_stop, f"{line}||{stop_name}", delay_s)

            message_count += 1
            if message_count % SAVE_EVERY == 0:
                print_summary()
                save_gold(conn)

    except KeyboardInterrupt:
        logger.info("Gold consumer stopped.")
    finally:
        save_gold(conn)
        conn.close()
        consumer.close()


if __name__ == "__main__":
    run_gold_consumer()