import json
import logging
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

TOPIC          = "bvg-departures"
WINDOW_MINUTES = 5      # rolling window size
PRINT_EVERY    = 10     # print summary every N messages consumed

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

    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # wait up to 1s for a message

            if msg is None:
                continue  # no message in this poll, keep waiting

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — not an error, just caught up
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

            delay     = value.get("delay")       # seconds or None
            line      = value.get("line")
            stop_name = value.get("stopName")

            # Skip if essential fields are missing or no real-time delay data
            if delay is None or not line or not stop_name:
                continue

            # ── Update rolling windows ────────────────────────────────────────
            evict_old_entries(by_line,      WINDOW_MINUTES)
            evict_old_entries(by_line_stop, WINDOW_MINUTES)

            add_entry(by_line,      line,                delay)
            add_entry(by_line_stop, f"{line}||{stop_name}", delay)

            # ── Print summary every N messages ────────────────────────────────
            message_count += 1
            if message_count % PRINT_EVERY == 0:
                print_summary()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()