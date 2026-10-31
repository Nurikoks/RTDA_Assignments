"""Part E: throughput and latency under synthetic load.

The producer and the consumer run as two separate processes. Every synthetic
event carries the time it was sent (sent_ns). The consumer does the same work
as consumer.py (JSON parse, schema check, counts, 5 s windows) and records
end-to-end latency = time processed - sent_ns.

python load_test.py --rates 10 100 500 --duration 20
python load_test.py --rates 0 --duration 10          (0 = no rate limit, finds the maximum)
python load_test.py --rates 500 --batch-size 1 --commit message   (slow variant for comparison)
"""
import argparse
import csv
import json
import multiprocessing as mp
import random
import statistics
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import psutil
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from stream_common import BOOTSTRAP, validate_event

OUT = Path(__file__).parent / "outputs"
GROUPS = ["G01", "G02", "G03", "G04", "G05", "G06"]
JOBS = ["simulation", "ml", "image", "text"]


def make_topic(name, partitions):
    # every run gets a new topic name, topics are not deleted (deleting crashes the broker on Windows)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[name].result()


def run_producer(topic, rate, n_events, ready, result):
    ready.wait()
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all",
                  "enable.idempotence": True, "linger.ms": 5})
    rnd = random.Random(42)
    t0 = time.perf_counter()
    cpu0 = time.process_time()
    for i in range(n_events):
        if rate:
            wait = t0 + i / rate - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
        group = rnd.choice(GROUPS)
        event = {
            "event_id": f"L{i:07d}",
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "research_group": group,
            "job_type": rnd.choice(JOBS),
            "duration_sec": rnd.randint(10, 130),
            "status": "failed" if rnd.random() < 0.15 else "completed",
            "cpu_pct": rnd.randint(30, 99),
            "sent_ns": time.time_ns(),
        }
        while True:
            try:
                p.produce(topic, key=group, value=json.dumps(event))
                break
            except BufferError:          # local queue is full, wait for the broker
                p.poll(0.01)
        p.poll(0)
    p.flush(30)
    wall = time.perf_counter() - t0
    result.put(("producer", {"sent": n_events, "wall": wall,
                             "cpu_pct": 100 * (time.process_time() - cpu0) / wall}))


def run_consumer(topic, n_events, batch_size, batch_timeout, commit_mode, ready, result):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"load-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic], on_assign=lambda cons, parts: (cons.assign(parts), ready.set()))

    latencies = []
    counts = defaultdict(int)
    windows = defaultdict(lambda: [0, 0, 0])       # window start -> [n, failed, cpu_sum]
    invalid = 0
    first = last = None
    cpu0 = time.process_time()
    idle_since = time.perf_counter()
    while len(latencies) + invalid < n_events and time.perf_counter() - idle_since < 15:
        if batch_size == 1:
            m = c.poll(batch_timeout)
            msgs = [m] if m is not None else []
        else:
            msgs = c.consume(num_messages=batch_size, timeout=batch_timeout)
        if not msgs:
            continue
        idle_since = time.perf_counter()
        for m in msgs:
            if m.error():
                continue
            event = json.loads(m.value())
            if validate_event(event):
                invalid += 1
                continue
            counts[(event["job_type"], event["status"])] += 1
            ts = datetime.fromisoformat(event["timestamp"])
            w = windows[ts.replace(microsecond=0, second=ts.second // 5 * 5)]
            w[0] += 1
            w[1] += event["status"] == "failed"
            w[2] += event["cpu_pct"]
            now = time.time_ns()
            latencies.append((now - event["sent_ns"]) / 1e6)
            first = first or time.perf_counter()
            last = time.perf_counter()
            if commit_mode == "message":
                c.commit(message=m, asynchronous=False)
        if commit_mode == "batch":
            c.commit(asynchronous=True)
    cpu = time.process_time() - cpu0
    c.close()
    span = (last - first) if first and last and last > first else float("nan")
    lat = sorted(latencies)
    result.put(("consumer", {
        "received": len(latencies), "invalid": invalid, "windows": len(windows),
        "span": span,
        "avg_latency": statistics.mean(lat) if lat else float("nan"),
        "p50_latency": lat[len(lat) // 2] if lat else float("nan"),
        "p95_latency": lat[int(len(lat) * 0.95)] if lat else float("nan"),
        "max_latency": lat[-1] if lat else float("nan"),
        "cpu_sec": cpu,
    }))


def one_run(rate, duration, partitions, batch_size, batch_timeout, commit_mode, max_events):
    topic = f"aitu.research.load.{uuid.uuid4().hex[:6]}"
    make_topic(topic, partitions)
    n = max_events if rate == 0 else int(rate * duration)
    ready, result = mp.Event(), mp.Queue()
    cons = mp.Process(target=run_consumer,
                      args=(topic, n, batch_size, batch_timeout, commit_mode, ready, result))
    prod = mp.Process(target=run_producer, args=(topic, rate, n, ready, result))
    cons.start()
    ready.wait(30)
    time.sleep(1)                      # let the assignment settle
    psutil.cpu_percent(None)
    t0 = time.perf_counter()
    prod.start()
    out = dict(result.get(timeout=600) for _ in range(2))
    wall = time.perf_counter() - t0
    system_cpu = psutil.cpu_percent(None)
    prod.join()
    cons.join()

    pr, co = out["producer"], out["consumer"]
    return {
        "input_rate": rate or "max",
        "events": n,
        "received": co["received"],
        "producer_rate": round(pr["sent"] / pr["wall"], 1),
        "observed_throughput": round(co["received"] / co["span"], 1) if co["span"] == co["span"] else "",
        "avg_latency_ms": round(co["avg_latency"], 2),
        "p50_latency_ms": round(co["p50_latency"], 2),
        "p95_latency_ms": round(co["p95_latency"], 2),
        "max_latency_ms": round(co["max_latency"], 2),
        "consumer_cpu_pct": round(100 * co["cpu_sec"] / co["span"], 1) if co["span"] == co["span"] else "",
        "producer_cpu_pct": round(pr["cpu_pct"], 1),
        "system_cpu_pct": system_cpu,
        "wall_sec": round(wall, 2),
        "batch_size": batch_size,
        "batch_timeout_s": batch_timeout,
        "commit": commit_mode,
        "partitions": partitions,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", type=int, nargs="+", default=[10, 100, 500])
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--batch-timeout", type=float, default=1.0)
    ap.add_argument("--commit", choices=["batch", "message"], default="batch")
    ap.add_argument("--max-events", type=int, default=50000, help="events for the unthrottled run")
    ap.add_argument("--out", default="partE_load_results.csv")
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)

    print(f"machine: {psutil.cpu_count(logical=False)} cores / {psutil.cpu_count()} threads, "
          f"RAM {psutil.virtual_memory().total / 2**30:.1f} GB")
    rows = []
    for rate in args.rates:
        row = one_run(rate, args.duration, args.partitions, args.batch_size,
                      args.batch_timeout, args.commit, args.max_events)
        print(row)
        rows.append(row)

    path = OUT / args.out
    new = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        if new:
            w.writeheader()
        w.writerows(rows)
    print("saved to", path)


if __name__ == "__main__":
    main()
