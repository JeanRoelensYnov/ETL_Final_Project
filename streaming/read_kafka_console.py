import spark_env  # noqa: F401  (pose JAVA_HOME / HADOOP_HOME — DOIT être importé en 1er)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "topic_cours_bourse"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Schéma explicite du message JSON (Spark a besoin d'un schéma, pas d'inférence en streaming).
SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("read_kafka_console")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "2")   # petit volume -> peu de partitions
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # readStream depuis Kafka : la valeur est binaire -> CAST en string -> from_json
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
        .format("console")
        .option("truncate", False)
        .trigger(availableNow=True)        # traite tout l'existant puis s'arrête
        .start()
    )
    query.awaitTermination()
    spark.stop()
    print("OK - lecture Kafka terminée")


if __name__ == "__main__":
    main()
