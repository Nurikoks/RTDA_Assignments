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

| File | What it is |
|---|---|
| assignment4_kafka_spark.ipynb | the whole assignment: topics (A), producers and consumer (B), windows (C), Spark (D), load test results (E), figures |
| load_test.py | Part E load test and consumer-group scaling test, plus the schema check `validate_event()` |
| start_kafka.ps1 | starts the Kafka broker, `-Reset` gives an empty broker |
| winfs/aitu/*.java | small Hadoop file system class so Spark runs on Windows without winutils.exe |
| data/research_events.csv | the 10 given events |
| outputs/ | result tables (CSV) and load test logs |
| figures/ | architecture diagram, window chart, load chart |
| Assignment_4_Report_completed.docx | the completed report |

`load_test.py` is a separate file because the tests start producers and consumers in
separate processes, and on Windows a process can not run a function defined in a notebook cell.

## How to run

```powershell
pip install -r requirements.txt
.\start_kafka.ps1 -Reset          # in its own terminal, keep it open
jupyter notebook assignment4_kafka_spark.ipynb    # Run All
```

The notebook needs an empty broker, because topics can not be deleted on Windows.
Part E shows the saved results. To measure again set `RUN_LOAD_TESTS = True`, or run:

```powershell
python load_test.py --rates 10 100 500 --duration 20
python load_test.py --rates 10 100 500 --duration 20 --batch-timeout 0.05
python load_test.py --rates 0 --max-events 50000
python load_test.py --rates 500 0 --duration 20 --max-events 20000 --batch-size 1 --batch-timeout 0.05 --commit message
python load_test.py --scaling --events 200000 --consumers 1 2 3
```

## Windows problems I had

1. Kafka `.bat` scripts fail with "The input line is too long". Kafka is started with `java -cp libs\*` directly (`start_kafka.ps1`).
2. Deleting a topic crashes the broker on Windows (`AccessDeniedException` when it renames the log folder). Topics are never deleted, a clean run uses `start_kafka.ps1 -Reset`.
3. With the default `auto.create.topics.enable=true` the producers created the topic with 1 partition before it was created with 3. Auto creation is now disabled.
4. Spark needs `winutils.exe` for chmod, readlink and directory listing. Instead of a third-party binary, `winfs/aitu/NoPermLocalFileSystem.java` skips these calls (compiled automatically with `javac`), and the Kafka connector jars are put on the class path instead of `spark.jars.packages`.
