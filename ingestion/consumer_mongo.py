import json
import os
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient

# Rend config.py (à la racine du projet) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BOOTSTRAP, MONGO_URI

TOPIC_NEWS = "topic_actualites_finance"
TOPIC_EVENTS = "topic_evenements_mondiaux"

# On déduit une catégorie lisible à partir du topic d'origine.
CATEGORIE = {
    TOPIC_NEWS: "finance",
    TOPIC_EVENTS: "evenement",
}


def to_document(record) -> dict:
    record_copy = dict(record.value)
    record_copy["categorie"] = CATEGORIE[record.topic]
    record_copy["ingested_at"] = datetime.now(timezone.utc)
    return record_copy


def main() -> None:
    client = MongoClient(MONGO_URI)
    collection = client["bourse"]["actualites"]

    consumer = KafkaConsumer(
        TOPIC_NEWS, TOPIC_EVENTS,                 # on s'abonne aux DEUX topics
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mongo-sink",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=8000,
    )

    print("En écoute sur les topics d'actualités...")
    count = 0
    for record in consumer:
        doc = to_document(record)
        # UPSERT par 'link' : le lien d'un article est unique -> pas de doublon
        # si on rejoue le pipeline (équivalent NoSQL de ON DUPLICATE KEY UPDATE).
        collection.update_one({"link": doc["link"]}, {"$set": doc}, upsert=True)
        count += 1

    print(f"{count} article(s) traité(s). Total en base : {collection.count_documents({})}")
    client.close()


if __name__ == "__main__":
    main()
