import json
from datetime import datetime, timezone

import feedparser
import yfinance as yf
from kafka import KafkaProducer

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:9092"      

TOPIC_COURS = "topic_cours_bourse"
TOPIC_NEWS = "topic_actualites_finance"

SYMBOL = "AAPL"
RSS_URL = "https://services.lesechos.fr/rss/les-echos-finance-marches.xml"

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
        "timestamp": df.index[-1].isoformat(),   # pandas Timestamp -> str ISO
        "open": float(last["Open"]),              # float() : numpy.float64 -> float JSON
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
        "volume": int(last["Volume"]),            # int() : numpy.int64 -> int JSON
    }


# ---------------------------------------------------------------------
# Programme principal
# ---------------------------------------------------------------------
def main() -> None:
    producer = make_producer()
    print("Producteur connecté à Kafka.\n")

    print(f"Actualités RSS ({RSS_URL}) :")
    for article in fetch_news(RSS_URL)[:5]:        
        send(producer, TOPIC_NEWS, article)

    print(f"\nCours de bourse ({SYMBOL}) :")
    quote = fetch_quote(SYMBOL)
    if quote:
        send(producer, TOPIC_COURS, quote)

    producer.flush()     
    producer.close()
    print("\nTerminé.")


if __name__ == "__main__":
    main()
