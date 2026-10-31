"""Create the Kafka topics: 3 partitions for job events and 1 for the dead-letter queue.

python create_topic.py

Topics are never deleted from here. On Windows the broker crashes when it
deletes a topic (it can not rename the memory-mapped log files, AccessDeniedException).
For a clean run stop the broker, empty the data folder and format it again
(see README.md).
"""
import argparse

from confluent_kafka.admin import AdminClient, NewTopic

from stream_common import BOOTSTRAP, DLQ_TOPIC, PARTITIONS, REPLICATION, TOPIC


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--partitions", type=int, default=PARTITIONS)
    args = ap.parse_args()

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topics = [args.topic, DLQ_TOPIC]
    new = [
        NewTopic(args.topic, num_partitions=args.partitions, replication_factor=REPLICATION,
                 config={"retention.ms": str(7 * 24 * 3600 * 1000)}),
        NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=REPLICATION),
    ]
    existing = admin.list_topics(timeout=10).topics
    missing = [n for n in new if n.topic not in existing]
    if missing:
        for t, fut in admin.create_topics(missing).items():
            fut.result()
            print("created", t)

    md = admin.list_topics(timeout=10)
    for t in topics:
        parts = md.topics[t].partitions
        print(f"{t}: partitions={len(parts)} leaders={[p.leader for p in parts.values()]} "
              f"replicas={[len(p.replicas) for p in parts.values()]}")


if __name__ == "__main__":
    main()
