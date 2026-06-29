import json
import os
import sys
from datetime import datetime, timezone

import feedparser
import yfinance as yf
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BOOTSTRAP
from symbols import ALL_SYMBOLS

sys.stdout.reconfigure(encoding="utf-8", errors="replace")


TOPIC_COURS = "topic_cours_bourse"
TOPIC_NEWS = "topic_actualites_finance"
TOPIC_EVENTS = "topic_evenements_mondiaux"

# Chaque flux RSS est routé vers un topic selon sa nature :
#   marchés / finance          -> topic_actualites_finance
#   économie / monde / politique -> topic_evenements_mondiaux
FEEDS = [
    ("https://services.lesechos.fr/rss/les-echos-finance-marches.xml", TOPIC_NEWS),
    ("https://feeds.a.dj.com/rss/RSSMarketsMain.xml",                  TOPIC_NEWS),
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html",         TOPIC_NEWS),
    ("https://www.lemonde.fr/economie/rss_full.xml",                  TOPIC_EVENTS),
    ("https://news.google.com/rss/search?q=%C3%A9conomie+politique+r%C3%A9glementation&hl=fr&gl=FR&ceid=FR:fr",
     TOPIC_EVENTS),
]

# Beaucoup de sites renvoient 403 au user-agent par défaut de feedparser :
# on se fait passer pour un navigateur.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


# ---------------------------------------------------------------------
# Connexion Kafka
# ---------------------------------------------------------------------
def make_producer() -> KafkaProducer:
    """Crée un producteur qui sérialise automatiquement les dicts en JSON."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )


def send(producer: KafkaProducer, topic: str, record: dict) -> None:
    """Envoie un enregistrement dans un topic et l'affiche pour le suivi."""
    producer.send(topic, record)
    print(f"  → [{topic}] {record}")


def fetch_news(rss_url: str) -> list[dict]:
    """Lit un flux RSS et renvoie une liste d'actualités normalisées."""
    feed = feedparser.parse(rss_url, agent=USER_AGENT)
    articles = []
    for entry in feed.entries:
        articles.append({
            "source": feed.feed.get("title", rss_url),
            "title": entry.get("title", ""),
            "summary": entry.get("summary", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
        })
    return articles


def fetch_quote(symbol: str) -> dict | None:
    df = yf.Ticker(symbol).history(period="1d", interval="1m")
    if df.empty:
        return None
    last = df.iloc[-1]
    return {
        "symbol": symbol,
        "timestamp": df.index[-1].isoformat(),   
        "open": float(last["Open"]),              
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
        "volume": int(last["Volume"]),            
    }


# ---------------------------------------------------------------------
# Programme principal
# ---------------------------------------------------------------------
def main() -> None:
    producer = make_producer()
    print("Producteur connecté à Kafka.\n")

    for url, topic in FEEDS:
        articles = fetch_news(url)
        print(f"\n{len(articles)} articles depuis {url}\n  -> {topic}")
        for article in articles[:10]:          # on plafonne par flux
            send(producer, topic, article)

    print(f"\nCours de bourse ({len(ALL_SYMBOLS)} symboles) :")
    for symbol in ALL_SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            send(producer, TOPIC_COURS, quote)

    producer.flush()
    producer.close()
    print("\nTerminé.")


if __name__ == "__main__":
    main()
