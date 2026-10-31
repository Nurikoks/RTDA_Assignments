"""Part D: Spark Structured Streaming job that reads the same Kafka topic.

Output: window, job_type, count, avg_duration for 5-second event-time windows.

python spark_streaming_job.py                 -> read everything in the topic and stop (availableNow)
python spark_streaming_job.py --live 60       -> keep running for 60 s with a 2 s micro-batch trigger
"""
import argparse
import os
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import IntegerType, StringType, StructField, StructType  # noqa: E402

from stream_common import BOOTSTRAP, JOB_TYPES, STATUSES, TOPIC  # noqa: E402

OUT = Path(__file__).parent / "outputs"

# Spark Kafka connector and its dependencies (Maven Central).
# They are put on the driver class path instead of spark.jars.packages, because
# on Windows spark.jars.packages calls chmod on the jars, which needs winutils.exe.
MAVEN = "https://repo1.maven.org/maven2"
KAFKA_JARS = [
    "org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.1/spark-sql-kafka-0-10_2.13-4.2.1.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.1/spark-token-provider-kafka-0-10_2.13-4.2.1.jar",
    "org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar",
    "org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar",
]
JAR_DIR = Path.home() / ".spark-kafka-jars"     # ASCII path, the JVM class path does not like Cyrillic


def kafka_jars():
    JAR_DIR.mkdir(exist_ok=True)
    paths = []
    for rel in KAFKA_JARS:
        target = JAR_DIR / rel.rsplit("/", 1)[1]
        if not target.exists():
            print("downloading", rel)
            urllib.request.urlretrieve(f"{MAVEN}/{rel}", target)
        paths.append(str(target))
    if os.name == "nt":
        paths.append(windows_fs_jar())
    return os.pathsep.join(paths)


def windows_fs_jar():
    """Compile winfs/aitu/*.java once (needs javac from the JDK).

    Without winutils.exe Hadoop fails when Spark creates the checkpoint folder,
    because it tries to chmod it. These classes skip chmod on the local disk.
    """
    jar = JAR_DIR / "aitu-winfs.jar"
    if jar.exists():
        return str(jar)
    import pyspark
    hadoop_api = next((Path(pyspark.__file__).parent / "jars").glob("hadoop-client-api-*.jar"))
    src = sorted((Path(__file__).parent / "winfs" / "aitu").glob("*.java"))
    build = Path(tempfile.mkdtemp(prefix="aitu_winfs_"))
    subprocess.run(["javac", "--release", "17", "-cp", str(hadoop_api), "-d", str(build),
                    *map(str, src)], check=True)
    with zipfile.ZipFile(jar, "w") as z:
        for f in build.rglob("*.class"):
            z.write(f, f.relative_to(build).as_posix())
    return str(jar)


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("research_group", StringType()),
    StructField("job_type", StringType()),
    StructField("duration_sec", IntegerType()),
    StructField("status", StringType()),
    StructField("cpu_pct", IntegerType()),
])


def build_query(spark, topic):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .load())

    events = (raw
              .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("event_time", F.to_timestamp("timestamp"))
              # same schema rules as the Python consumer, broken rows are dropped
              .filter(F.col("event_id").isNotNull() & F.col("event_time").isNotNull()
                      & F.col("job_type").isin(*JOB_TYPES) & F.col("status").isin(*STATUSES)
                      & (F.col("duration_sec") > 0) & F.col("cpu_pct").between(0, 100)))

    return (events
            .groupBy(F.window("event_time", "5 seconds"), F.col("job_type"))
            .agg(F.count("*").alias("count"),
                 F.round(F.avg("duration_sec"), 2).alias("avg_duration")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--live", type=int, default=0, help="run N seconds with a 2 s trigger")
    ap.add_argument("--out", default="partD_spark_output.csv")
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)

    spark = (SparkSession.builder
             .appName("aitu-research-job-windows")
             .master("local[2]")
             .config("spark.driver.extraClassPath", kafka_jars())
             .config("spark.hadoop.fs.file.impl", "aitu.NoPermLocalFileSystem" if os.name == "nt"
                     else "org.apache.hadoop.fs.LocalFileSystem")
             .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "aitu.NoPermLocalFs" if os.name == "nt"
                     else "org.apache.hadoop.fs.local.LocalFs")
             .config("spark.sql.shuffle.partitions", "3")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    result = build_query(spark, args.topic)
    with open(OUT / "partD_spark_schema.txt", "w") as f:
        f.write(result._jdf.schema().treeString())
    print(result._jdf.schema().treeString())

    def save_batch(batch_df, batch_id):
        pdf = (batch_df
               .select(F.date_format("window.start", "HH:mm:ss").alias("window_start"),
                       F.date_format("window.end", "HH:mm:ss").alias("window_end"),
                       "job_type", "count", "avg_duration")
               .orderBy("window_start", "job_type")
               .toPandas())
        print(f"--- micro-batch {batch_id}: {len(pdf)} rows")
        print(pdf.to_string(index=False))
        if len(pdf):
            pdf.to_csv(OUT / args.out, index=False)

    checkpoint = tempfile.mkdtemp(prefix="aitu_spark_ckpt_")
    writer = (result.writeStream
              .outputMode("complete")
              .foreachBatch(save_batch)
              .option("checkpointLocation", checkpoint))
    if args.live:
        query = writer.trigger(processingTime="2 seconds").start()
        query.awaitTermination(args.live)
        query.stop()
    else:
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    print("last progress:", query.lastProgress and {
        k: query.lastProgress[k] for k in ("batchId", "numInputRows", "durationMs")})
    spark.stop()


if __name__ == "__main__":
    main()
