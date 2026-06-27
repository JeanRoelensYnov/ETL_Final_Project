import json
import os
import sys
from datetime import datetime, timezone

import pymysql
from kafka import KafkaConsumer

# Rend config.py (à la racine du projet) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BOOTSTRAP, MYSQL_CONF

TOPIC_COURS = "topic_cours_bourse"

# INSERT idempotent : si (symbol, ts) existe déjà (clé UNIQUE), on met à jour
# au lieu de créer un doublon. -> on peut rejouer le pipeline sans le polluer.
INSERT_SQL = """
    INSERT INTO cours_bourse (symbol, ts, open, high, low, close, volume)
    VALUES (%(symbol)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low),
        close=VALUES(close), volume=VALUES(volume)
"""


def to_utc_naive(iso_ts: str) -> datetime:
    """'2026-06-25T15:59:00-04:00' -> datetime UTC sans fuseau (pour MySQL DATETIME)."""
    dt = datetime.fromisoformat(iso_ts)          # garde le fuseau d'origine
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def to_row(msg: dict) -> dict:
    """Transforme un message Kafka en ligne prête pour l'INSERT."""
    return {
        "symbol": msg["symbol"],
        "ts": to_utc_naive(msg["timestamp"]),
        "open": msg["open"],
        "high": msg["high"],
        "low": msg["low"],
        "close": msg["close"],
        "volume": msg["volume"],
    }


def main() -> None:
    conn = pymysql.connect(**MYSQL_CONF)
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        TOPIC_COURS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mysql-sink",                 # groupe nommé -> les offsets sont mémorisés
        auto_offset_reset="earliest",          # 1re fois : lire tout l'historique
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=8000,              # s'arrête après 8 s sans nouveau message (démo)
    )

    print(f"En écoute sur '{TOPIC_COURS}'... (Ctrl+C pour arrêter)")
    count = 0
    for record in consumer:
        row = to_row(record.value)
        cursor.execute(INSERT_SQL, row)
        conn.commit()
        count += 1
        print(f"  inséré : {row['symbol']} @ {row['ts']} close={row['close']}")

    print(f"\n{count} message(s) traité(s). Fin.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
