import os
import sys

# Rend config.py (à la racine du projet) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import spark_env  # noqa: F401  (JAVA_HOME / HADOOP_HOME — en premier)

from datetime import datetime, timezone

import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)

from config import KAFKA_BOOTSTRAP, MYSQL_CONF

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TOPIC = "topic_cours_bourse"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
CHECKPOINT = "checkpoints/cours_mysql"      # gitignored

INSERT_SQL = """
    INSERT INTO cours_bourse (symbol, ts, open, high, low, close, volume)
    VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low),
        close=VALUES(close), volume=VALUES(volume)
"""

SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
])


def to_utc_naive(iso_ts: str) -> datetime:
    return datetime.fromisoformat(iso_ts).astimezone(timezone.utc).replace(tzinfo=None)


def write_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()              # volume faible -> collect sur le driver est OK
    if not rows:
        return
    data = [{
        "symbol": r["symbol"],
        "ts": to_utc_naive(r["timestamp"]),
        "open": r["open"], "high": r["high"], "low": r["low"],
        "close": r["close"], "volume": r["volume"],
    } for r in rows]

    conn = pymysql.connect(**MYSQL_CONF)
    cursor = conn.cursor()
    cursor.executemany(INSERT_SQL, data)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  batch {batch_id} : {len(data)} ligne(s) -> MySQL")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("stream_to_mysql")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    cours = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), SCHEMA).alias("d"))
        .select("d.*")
    )

    query = (
        cours.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)        # traite tout l'existant puis s'arrête
        .start()
    )
    query.awaitTermination()
    spark.stop()
    print("OK - streaming Kafka -> MySQL terminé")


if __name__ == "__main__":
    main()
