# streaming/kafka_producer.py
# ─────────────────────────────────────────────────────────────────────
# Kafka Producer: reads batch_sales CSV row-by-row and publishes
# each row as a JSON message to sales_topic at ~1 msg/second.
# ─────────────────────────────────────────────────────────────────────

import os
import sys
import json
import time
import logging
import datetime

import pandas as pd
from confluent_kafka import Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC,
    DATASET_LOCAL_PATH, PRODUCER_DELAY_SEC,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def delivery_report(err, msg) -> None:
    """Called by producer once message is delivered (or fails)."""
    if err:
        log.error("Delivery failed: %s", err)
    else:
        log.debug("Delivered → topic=%s partition=%d offset=%d",
                  msg.topic(), msg.partition(), msg.offset())


def serialize_row(row: pd.Series) -> dict:
    """Convert a DataFrame row to a JSON-serialisable dict."""
    rec = row.to_dict()
    for k, v in rec.items():
        if isinstance(v, (pd.Timestamp, datetime.datetime)):
            rec[k] = v.isoformat()
        elif pd.isna(v):
            rec[k] = None
        elif hasattr(v, "item"):          # numpy scalar → python native
            rec[k] = v.item()
    rec["kafka_ts"] = datetime.datetime.utcnow().isoformat()
    return rec


def run_producer() -> None:
    log.info("Loading dataset from %s …", DATASET_LOCAL_PATH)
    df = pd.read_csv(DATASET_LOCAL_PATH, low_memory=False)
    log.info("Dataset rows: %d", len(df))

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    log.info("Connected to Kafka at %s, topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    log.info("Starting stream — one message every %ss  (Ctrl+C to stop)",
             PRODUCER_DELAY_SEC)

    sent = 0
    try:
        # Loop infinitely so the stream never stops
        while True:
            for _, row in df.iterrows():
                payload = serialize_row(row)
                producer.produce(
                    KAFKA_TOPIC,
                    key=str(payload.get("order_id", sent)),
                    value=json.dumps(payload),
                    callback=delivery_report,
                )
                producer.poll(0)        # trigger delivery callbacks
                sent += 1

                if sent % 100 == 0:
                    log.info("Messages sent: %d", sent)

                time.sleep(PRODUCER_DELAY_SEC)

    except KeyboardInterrupt:
        log.info("Stopping producer … flushing remaining messages.")
    finally:
        producer.flush()
        log.info("Producer done. Total messages sent: %d", sent)


if __name__ == "__main__":
    run_producer()
