# Assignment 4: Distributed Stream Processing (Kafka + Spark)

Kaziyev Nurassyl, BDA-2401. Synthetic AITU research-job events.

## Setup used

- Windows 11, Python 3.13.5, Java 21.0.6
- Apache Kafka 4.3.2 (Scala 2.13), single broker in KRaft mode, `localhost:9092`
- confluent-kafka 2.16.1, PySpark 4.2.1 (Spark Kafka connector 4.2.1, kafka-clients 3.9.2)

Kafka topic configuration:

| Topic | Partitions | Replication | Notes |
|---|---|---|---|
| aitu.research.jobs | 3 | 1 | key = research_group, retention 7 days |
| aitu.research.jobs.dlq | 1 | 1 | events that fail schema validation |

Broker config `aitu-server.properties` = Kafka's `server.properties` with
`log.dirs=C:/Users/kazie/kafka/data` and `auto.create.topics.enable=false`.

## Files

| File | Part | What it does |
|---|---|---|
| data/research_events.csv | | the 10 given events |
| stream_common.py | B | topic names, schema check `validate_event()` |
| create_topic.py | A | creates the 3-partition topic and the DLQ topic |
| producer.py | B | JSON producer, key = research_group, `--group` runs one producer per group |
| consumer.py | B, C | consumer group `aitu-analytics`: validation, counts, averages, 5 s event-time windows |
| spark_streaming_job.py | D | Spark Structured Streaming: window, job_type, count, avg_duration |
| winfs/aitu/*.java | D | small Hadoop file system class so Spark runs on Windows without winutils.exe |
| load_test.py | E | throughput / latency / CPU at 10, 100, 500 events/s |
| scaling_test.py | E | 1, 2, 3 consumers in one group on a preloaded topic |
| make_figures.py | | figures in `figures/` |
| outputs/ | | logs and CSV results of the runs |

## How to run

```powershell
pip install -r requirements.txt
.\start_kafka.ps1 -Reset          # in its own terminal, keep it open
python create_topic.py
# three producers at the same time, one per research group
Start-Process python "producer.py --group G01 --delay 1"
Start-Process python "producer.py --group G02 --delay 1"
Start-Process python "producer.py --group G03 --delay 1"
python consumer.py --from-beginning
python spark_streaming_job.py
python load_test.py --rates 10 100 500 --duration 20
python load_test.py --rates 10 100 500 --duration 20 --batch-timeout 0.05
python load_test.py --rates 0 --max-events 50000
python scaling_test.py --events 200000 --consumers 1 2 3
python make_figures.py
```

Validation test (3 broken events go to the DLQ):

```powershell
python create_topic.py --topic aitu.research.jobs.validation-test
python producer.py --topic aitu.research.jobs.validation-test --inject-invalid --delay 0
python consumer.py --topic aitu.research.jobs.validation-test --group-id validation-test --from-beginning
```

## Windows problems I had

1. Kafka `.bat` scripts fail with "The input line is too long". Kafka is started with `java -cp libs\*` directly (`start_kafka.ps1`).
2. Deleting a topic crashes the broker on Windows (`AccessDeniedException` when it renames the log folder). Topics are never deleted, a clean run uses `start_kafka.ps1 -Reset`.
3. With the default `auto.create.topics.enable=true` the producers created the topic with 1 partition before `create_topic.py` ran. Auto creation is now disabled.
4. Spark needs `winutils.exe` for chmod, readlink and directory listing. Instead of a third-party binary, `winfs/aitu/NoPermLocalFileSystem.java` skips these calls (compiled automatically with `javac`), and the Kafka connector jars are put on the class path instead of `spark.jars.packages`.
