"""Kafka consumer and Python stream processor (Part B and Part C).

For every event it
  1. parses the JSON and checks the schema (bad events go to the DLQ topic),
  2. updates completed/failed counts and average duration per job_type,
  3. updates average CPU per research group,
  4. puts the event into a 5-second event-time tumbling window [start, end).

A window is closed when the watermark passes its end. The watermark is the
minimum over partitions of the max event time, minus the allowed lateness. When the stream is idle the open windows are flushed.
Offsets are committed after each processed batch (at-least-once).

python consumer.py --from-beginning
"""
import argparse
import csv
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Producer

from stream_common import BOOTSTRAP, CONSUMER_GROUP, DLQ_TOPIC, TOPIC, validate_event

OUT = Path(__file__).parent / "outputs"
WINDOW = timedelta(seconds=5)
ALLOWED_LATENESS = timedelta(seconds=2)


def window_start(ts):
    epoch = datetime(ts.year, ts.month, ts.day)
    sec = int((ts - epoch).total_seconds()) // 5 * 5
    return epoch + timedelta(seconds=sec)


class WindowAggregator:
    """5-second tumbling windows on event time with a per-partition watermark.

    Kafka keeps order only inside one partition, and the consumer may read one
    partition far ahead of the others. So the watermark is the MINIMUM over the
    partitions of (max event time in that partition), minus the allowed lateness.
    Until every assigned partition has sent data there is no watermark.
    """

    def __init__(self):
        self.open = {}          # window start -> state
        self.closed = []        # finished windows in order
        self.partition_max = {}  # partition -> max event time seen
        self.partitions = set()  # assigned partitions
        self.late = []

    @property
    def watermark(self):
        if not self.partitions or not self.partitions <= set(self.partition_max):
            return None
        return min(self.partition_max[p] for p in self.partitions) - ALLOWED_LATENESS

    def add(self, event, ts, partition):
        start = window_start(ts)
        if self.watermark is not None and start + WINDOW <= self.watermark:
            self.late.append(event["event_id"])     # its window is already closed
            return None
        w = self.open.setdefault(start, {"events": [], "failed": 0, "cpu_sum": 0.0})
        w["events"].append(event["event_id"])
        w["failed"] += event["status"] == "failed"
        w["cpu_sum"] += event["cpu_pct"]
        if ts > self.partition_max.get(partition, ts - WINDOW):
            self.partition_max[partition] = ts
        return start

    def close_ready(self, flush=False):
        done = []
        for start in sorted(self.open):
            if flush or (self.watermark is not None and start + WINDOW <= self.watermark):
                w = self.open.pop(start)
                n = len(w["events"])
                row = {
                    "window": f"{start:%H:%M:%S}-{start + WINDOW:%H:%M:%S}",
                    "events": " ".join(sorted(w["events"])),
                    "count": n,
                    "failed": w["failed"],
                    "failure_rate_pct": round(100 * w["failed"] / n, 2),
                    "avg_cpu": round(w["cpu_sum"] / n, 2),
                }
                self.closed.append(row)
                done.append(row)
        return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--group-id", default=CONSUMER_GROUP)
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--idle-sec", type=float, default=8.0, help="stop after this long without data")
    ap.add_argument("--out-prefix", default="")
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    dlq = Producer({"bootstrap.servers": BOOTSTRAP})

    assigned_at = []
    windows = WindowAggregator()

    def on_assign(c, partitions):
        assigned_at.append(time.time())
        windows.partitions = {p.partition for p in partitions}
        if args.from_beginning:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
        print("assigned partitions:", [p.partition for p in partitions])
        c.assign(partitions)

    consumer.subscribe([args.topic], on_assign=on_assign)

    status_count = defaultdict(lambda: {"completed": 0, "failed": 0})
    duration = defaultdict(list)
    cpu = defaultdict(list)
    valid = invalid = 0
    last_data = None
    started = time.time()

    # the idle timer starts only after the group join, joining can take a few seconds
    while time.time() - started < 60:
        batch = consumer.consume(num_messages=100, timeout=1.0)
        if not batch:
            if assigned_at and time.time() - (last_data or assigned_at[0]) > args.idle_sec:
                break
            continue
        last_data = time.time()
        for msg in batch:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("consumer error:", msg.error())
                continue
            try:
                event = json.loads(msg.value())
                errors = validate_event(event)
            except json.JSONDecodeError as e:
                event, errors = None, [f"not JSON: {e}"]
            if errors:
                invalid += 1
                dlq.produce(DLQ_TOPIC, value=msg.value(),
                            headers={"error": "; ".join(errors)})
                eid = event.get("event_id") if isinstance(event, dict) else "?"
                print(f"INVALID {eid} p{msg.partition()} off{msg.offset()} -> DLQ: {'; '.join(errors)}")
                continue

            valid += 1
            ts = datetime.fromisoformat(event["timestamp"])
            status_count[event["job_type"]][event["status"]] += 1
            duration[event["job_type"]].append(event["duration_sec"])
            cpu[event["research_group"]].append(event["cpu_pct"])
            start = windows.add(event, ts, msg.partition())
            where = "LATE (dropped)" if start is None else f"window {start:%H:%M:%S}"
            wm = windows.watermark
            print(f"recv {event['event_id']} p{msg.partition()} off{msg.offset()} "
                  f"event_time {ts:%H:%M:%S} -> {where}, "
                  f"watermark {'none yet' if wm is None else f'{wm:%H:%M:%S}'}")
            for row in windows.close_ready():
                print(f"  CLOSED window {row['window']}: {row}")
        consumer.commit(asynchronous=True)

    for row in windows.close_ready(flush=True):
        print(f"  FLUSHED window {row['window']}: {row}")
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        # _NO_OFFSET only means that the async commits already stored everything
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise
    consumer.close()
    dlq.flush(5)

    print(f"\nvalid events: {valid}, invalid events sent to DLQ: {invalid}, late events: {windows.late}")
    total = {s: sum(v[s] for v in status_count.values()) for s in ("completed", "failed")}
    print("completed / failed jobs:", total)

    order = ["ml", "image", "text", "simulation"]
    job_rows = [{"job_type": jt, **status_count[jt],
                 "avg_duration": round(sum(duration[jt]) / len(duration[jt]), 2)}
                for jt in order if jt in duration]
    group_rows = [{"research_group": g, "events": len(v), "avg_cpu": round(sum(v) / len(v), 2)}
                  for g, v in sorted(cpu.items())]

    print("\nPart B: by job_type")
    for r in job_rows:
        print(f"  {r['job_type']:<11} completed={r['completed']} failed={r['failed']} "
              f"avg_duration={r['avg_duration']}")
    print("Part B: average CPU by research group")
    for r in group_rows:
        print(f"  {r['research_group']}  events={r['events']} avg_cpu={r['avg_cpu']}")
    print("Part C: 5-second event-time windows")
    for r in windows.closed:
        print(f"  {r['window']}  events=[{r['events']}] count={r['count']} "
              f"failure_rate={r['failure_rate_pct']}% avg_cpu={r['avg_cpu']}")

    for name, rows in [("partB_job_types", job_rows), ("partB_cpu_by_group", group_rows),
                       ("partC_windows", windows.closed)]:
        if rows:
            with open(OUT / f"{args.out_prefix}{name}.csv", "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)


if __name__ == "__main__":
    main()
