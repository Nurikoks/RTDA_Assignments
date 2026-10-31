"""Shared settings and schema validation for the Assignment 4 pipeline."""
from datetime import datetime

BOOTSTRAP = "localhost:9092"
TOPIC = "aitu.research.jobs"
DLQ_TOPIC = "aitu.research.jobs.dlq"
PARTITIONS = 3
REPLICATION = 1           # one local broker, so 1 is the maximum here
CONSUMER_GROUP = "aitu-analytics"

# the CSV has only clock times, so one assumed date is added
EVENT_DATE = "2026-10-30"

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


def csv_row_to_event(row):
    """Turn one CSV row into the JSON event that is sent to Kafka."""
    return {
        "event_id": row["event_id"],
        "timestamp": f"{EVENT_DATE}T{row['timestamp']}",
        "research_group": row["research_group"],
        "job_type": row["job_type"],
        "duration_sec": int(row["duration_sec"]),
        "status": row["status"],
        "cpu_pct": int(row["cpu_pct"]),
    }


def validate_event(event):
    """Return a list of schema errors. An empty list means the event is valid."""
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
