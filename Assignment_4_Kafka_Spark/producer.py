"""Kafka producer for the AITU research-job events.

One producer process can stand for one research group (--group G01), so the
pipeline can be run with several producers at the same time, like separate
worker nodes. Key = research_group, so all events of one group go to the same
partition and keep their order.

python producer.py                    -> send all 10 events
python producer.py --group G02        -> send only the events of G02
python producer.py --inject-invalid   -> also send 3 broken events (validation test)
"""
import argparse
import csv
import json
import time
from pathlib import Path

from confluent_kafka import Producer

from stream_common import BOOTSTRAP, TOPIC, csv_row_to_event

DATA = Path(__file__).parent / "data" / "research_events.csv"

INVALID_EVENTS = [
    {"event_id": "X001", "timestamp": "2026-10-30T14:00:06", "research_group": "G02",
     "job_type": "audio", "duration_sec": 30, "status": "completed", "cpu_pct": 50},
    {"event_id": "X002", "timestamp": "2026-10-30T14:00:07", "research_group": "G01",
     "job_type": "ml", "duration_sec": 40, "status": "completed"},
    {"event_id": "X003", "timestamp": "2026-10-30T14:00:08", "research_group": "G03",
     "job_type": "text", "duration_sec": "fast", "status": "failed", "cpu_pct": 130},
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--group", help="send only this research group")
    ap.add_argument("--delay", type=float, default=0.1, help="seconds between events")
    ap.add_argument("--inject-invalid", action="store_true")
    args = ap.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": f"producer-{args.group or 'all'}",
        "acks": "all",
        "enable.idempotence": True,   # retries do not create duplicates
        "linger.ms": 5,
    })

    def on_delivery(err, msg):
        if err is not None:
            print(f"DELIVERY FAILED key={msg.key()} error={err}")
        else:
            event = json.loads(msg.value())
            print(f"sent {event.get('event_id')} key={msg.key().decode()} "
                  f"-> partition {msg.partition()} offset {msg.offset()}")

    with open(DATA, newline="") as f:
        events = [csv_row_to_event(r) for r in csv.DictReader(f)]
    if args.group:
        events = [e for e in events if e["research_group"] == args.group]
    if args.inject_invalid:
        events += INVALID_EVENTS

    for event in events:
        producer.produce(args.topic, key=event["research_group"],
                         value=json.dumps(event), on_delivery=on_delivery)
        producer.poll(0)
        time.sleep(args.delay)

    left = producer.flush(10)
    print(f"done: {len(events)} events, not delivered: {left}")


if __name__ == "__main__":
    main()
