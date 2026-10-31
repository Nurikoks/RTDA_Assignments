"""Extra Part E experiment: does the consumer group scale with more consumers?

A 3-partition topic is first filled with N synthetic events. Then 1, 2 and 3
consumer processes in one consumer group read it. Each consumer does the same
work as consumer.py (JSON parse, schema check, counts, 5 s windows). The
consumers wait (paused) until the group assignment is stable, then all start
at the same moment, and the drain throughput N / time is measured.

python scaling_test.py --events 200000 --consumers 1 2 3
"""
import argparse
import csv
import json
import multiprocessing as mp
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from stream_common import BOOTSTRAP, validate_event

OUT = Path(__file__).parent / "outputs"


def preload(topic, n):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})   # keep a reference until the result is back
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result()
    p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "acks": "all"})
    rnd = random.Random(7)
    for i in range(n):
        g = f"G{rnd.randint(1, 6):02d}"
        event = {"event_id": f"S{i:07d}", "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                 "research_group": g, "job_type": rnd.choice(["simulation", "ml", "image", "text"]),
                 "duration_sec": rnd.randint(10, 130),
                 "status": "failed" if rnd.random() < 0.15 else "completed",
                 "cpu_pct": rnd.randint(30, 99)}
        while True:
            try:
                p.produce(topic, key=g, value=json.dumps(event))
                break
            except BufferError:
                p.poll(0.01)
        p.poll(0)
    p.flush(60)


def consumer_proc(idx, topic, group, total, go, assigned, done_count, result):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})

    def on_assign(cons, parts):
        cons.assign(parts)
        if not go.is_set():
            cons.pause(parts)
        assigned[idx] = sorted(p.partition for p in parts)

    def on_revoke(cons, parts):
        assigned[idx] = []

    c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    while not go.is_set():
        c.poll(0.1)                     # keeps the group membership alive while paused
    my_parts = sorted(p.partition for p in c.assignment())
    c.resume(c.assignment())

    counts = defaultdict(int)
    windows = defaultdict(lambda: [0, 0, 0])
    n = 0
    first = last = None
    cpu0 = time.process_time()
    while done_count.value < total:
        msgs = c.consume(num_messages=500, timeout=0.05)
        if not msgs:
            continue
        first = first or time.time()
        for m in msgs:
            if m.error():
                continue
            event = json.loads(m.value())
            if validate_event(event):
                continue
            counts[(event["job_type"], event["status"])] += 1
            ts = datetime.fromisoformat(event["timestamp"])
            w = windows[ts.replace(microsecond=0, second=ts.second // 5 * 5)]
            w[0] += 1
            w[1] += event["status"] == "failed"
            w[2] += event["cpu_pct"]
            n += 1
        last = time.time()
        with done_count.get_lock():
            done_count.value += len(msgs)
        c.commit(asynchronous=True)
    cpu = time.process_time() - cpu0
    c.close()
    result.put({"idx": idx, "events": n, "first": first, "last": last, "cpu_sec": cpu,
                "partitions": my_parts})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=200000)
    ap.add_argument("--consumers", type=int, nargs="+", default=[1, 2, 3])
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)

    topic = f"aitu.research.scaling.{uuid.uuid4().hex[:6]}"
    t0 = time.time()
    preload(topic, args.events)
    print(f"preloaded {args.events} events into {topic} in {time.time() - t0:.1f} s")

    rows = []
    with mp.Manager() as mgr:
        for k in args.consumers:
            go, result = mp.Event(), mp.Queue()
            assigned = mgr.dict()
            done_count = mp.Value("i", 0)
            group = f"scaling-{k}-{uuid.uuid4().hex[:6]}"
            procs = [mp.Process(target=consumer_proc,
                                args=(i, topic, group, args.events, go, assigned, done_count, result))
                     for i in range(k)]
            for p in procs:
                p.start()
            # wait until all 3 partitions are assigned and the assignment does not change for 3 s
            stable_since, last_view = None, None
            while True:
                view = tuple(sorted((i, tuple(v)) for i, v in assigned.items()))
                full = sum(len(v) for _, v in view) == 3 and len(view) == k
                if full and view == last_view:
                    if time.time() - stable_since > 3:
                        break
                else:
                    stable_since, last_view = time.time(), view
                time.sleep(0.2)
            go.set()
            res = [result.get(timeout=600) for _ in procs]
            for p in procs:
                p.join()
            start = min(r["first"] for r in res if r["first"])
            end = max(r["last"] for r in res if r["last"])
            row = {
                "consumers": k,
                "events": sum(r["events"] for r in res),
                "drain_sec": round(end - start, 2),
                "throughput_eps": round(sum(r["events"] for r in res) / (end - start)),
                "per_consumer_events": " ".join(str(r["events"]) for r in sorted(res, key=lambda r: r["idx"])),
                "partitions": " ".join(str(r["partitions"]) for r in sorted(res, key=lambda r: r["idx"])),
                "avg_consumer_cpu_pct": round(sum(100 * r["cpu_sec"] / (end - start) for r in res) / k, 1),
            }
            print(row)
            rows.append(row)

    with open(OUT / "partE_scaling_results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
