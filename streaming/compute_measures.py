import os
import sys

# Rend config.py (à la racine du projet) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import spark_env  # noqa: F401  (JAVA_HOME / HADOOP_HOME — en premier)

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from config import JDBC_URL, JDBC_PROPS

MYSQL_PKG = "com.mysql:mysql-connector-j:8.4.0"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("compute_measures")
        .master("local[2]")
        .config("spark.jars.packages", MYSQL_PKG)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # --- Lecture de l'historique quotidien depuis MySQL (JDBC) ---
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "cours_journalier")
        .options(**JDBC_PROPS)
        .load()
    )

    # --- Fenêtres glissantes : par symbole, ordonnées dans le temps ---
    w = Window.partitionBy("symbol").orderBy("trade_date")
    w30 = w.rowsBetween(-29, 0)   # le jour courant + les 29 précédents
    w20 = w.rowsBetween(-19, 0)
    w50 = w.rowsBetween(-49, 0)

    # Rendement log + volatilité (écart-type des rendements log sur 30 jours)
    df = df.withColumn("prev_close", F.lag("close").over(w))
    df = df.withColumn("log_return", F.log(F.col("close") / F.col("prev_close")))
    df = df.withColumn("volatility_30", F.stddev("log_return").over(w30))

    # Moyennes mobiles
    df = df.withColumn("sma_20", F.avg("close").over(w20))
    df = df.withColumn("sma_50", F.avg("close").over(w50))
    df = df.withColumn("volume_ma_20", F.avg("volume").over(w20).cast("long"))

    # --- Tendance : haussière si la MM courte passe au-dessus de la MM longue ---
    df = df.withColumn(
        "trend",
        F.when(F.col("sma_20") > F.col("sma_50"), "haussiere").otherwise("baissiere"),
    )

    # --- Colonnes finales (doivent matcher la table `mesures`) ---
    result = df.select(
        "symbol", "trade_date", "log_return", "volatility_30",
        "sma_20", "sma_50", "volume_ma_20", "trend",
    )

    # --- Écriture dans `mesures` (truncate + append = recalcul idempotent) ---
    (
        result.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "mesures")
        .option("truncate", "true")
        .options(**JDBC_PROPS)
        .mode("overwrite")
        .save()
    )

    print("OK - mesures calculées et écrites")
    spark.stop()


if __name__ == "__main__":
    main()
