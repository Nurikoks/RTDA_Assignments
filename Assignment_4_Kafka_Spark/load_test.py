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

BOOTSTRAP = "localhost:9092"
JOB_TYPES = {"simulation", "ml", "image", "text"}
STATUSES = {"completed", "failed"}
FIELDS = {
    "event_id": str,
    "timestamp": str,
    "research_group": str,
    "job_type": str,
    "duration_sec": (int, float),
    "status": str,
    "cpu_pct": (int, float),
}


def validate_event(event):
    if not isinstance(event, dict):
        return ["event is not a JSON object"]
    errors = []
    for name, ftype in FIELDS.items():
        if name not in event:
            errors.append(f"missing field {name}")
        elif isinstance(event[name], bool) or not isinstance(event[name], ftype):
            errors.append(f"wrong type for {name}: {type(event[name]).__name__}")
    if errors:
        return errors
    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        errors.append(f"bad timestamp {event['timestamp']!r}")
    if event["job_type"] not in JOB_TYPES:
        errors.append(f"unknown job_type {event['job_type']!r}")
    if event["status"] not in STATUSES:
        errors.append(f"unknown status {event['status']!r}")
    if event["duration_sec"] <= 0:
        errors.append("duration_sec must be > 0")
    if not 0 <= event["cpu_pct"] <= 100:
        errors.append("cpu_pct must be in 0..100")
    return errors


OUT = Path(__file__).parent / "outputs"
GROUPS = ["G01", "G02", "G03", "G04", "G05", "G06"]
JOBS = ["simulation", "ml", "image", "text"]


def make_topic(name, partitions):
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
            except BufferError:
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
    windows = defaultdict(lambda: [0, 0, 0])
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
    time.sleep(1)
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


def preload(topic, n):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
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
        c.poll(0.1)
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


def run_scaling(n_events, consumer_counts):

    topic = f"aitu.research.scaling.{uuid.uuid4().hex[:6]}"
    t0 = time.time()
    preload(topic, n_events)
    print(f"preloaded {n_events} events into {topic} in {time.time() - t0:.1f} s")

    rows = []
    with mp.Manager() as mgr:
        for k in consumer_counts:
            go, result = mp.Event(), mp.Queue()
            assigned = mgr.dict()
            done_count = mp.Value("i", 0)
            group = f"scaling-{k}-{uuid.uuid4().hex[:6]}"
            procs = [mp.Process(target=consumer_proc,
                                args=(i, topic, group, n_events, go, assigned, done_count, result))
                     for i in range(k)]
            for p in procs:
                p.start()
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
    ap.add_argument("--scaling", action="store_true", help="run the consumer-group scaling test")
    ap.add_argument("--events", type=int, default=200000, help="events for the scaling test")
    ap.add_argument("--consumers", type=int, nargs="+", default=[1, 2, 3])
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)
    if args.scaling:
        run_scaling(args.events, args.consumers)
        return

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
