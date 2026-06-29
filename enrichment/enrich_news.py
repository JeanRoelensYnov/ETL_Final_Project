import os
import sys
from datetime import datetime, timezone

import pymysql
from pymongo import MongoClient

# Rend config.py (à la racine) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MONGO_URI, MYSQL_CONF

import llm

SAMPLE_SIZE = 20      # nombre d'articles à enrichir par exécution


def build_alias_map() -> dict[str, set]:
    """Construit {symbole: {mots-clés}} à partir de la table actif (+ alias manuels)."""
    conn = pymysql.connect(**MYSQL_CONF)
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, name FROM actif")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Noms peu évidents à reconnaître depuis le texte d'un article :
    manual = {
        "GOOGL": ["google", "alphabet"],
        "BTC-USD": ["bitcoin", "btc"],
        "ETH-USD": ["ethereum", "eth"],
        "^GSPC": ["s&p 500", "sp500"],
        "^FCHI": ["cac 40", "cac40"],
        "^GDAXI": ["dax"],
    }
    alias = {}
    for symbol, name in rows:
        kws = set()
        if name:
            kws.add(name.lower())
            kws.add(name.split()[0].lower())   # 1er mot, ex. "Apple Inc." -> "apple"
        kws.update(manual.get(symbol, []))
        alias[symbol] = kws
    return alias


def match_symbols(meta: dict, alias_map: dict[str, set]) -> list[str]:
    # Sorties LLM imprévisibles : on se protège contre clé absente / type inattendu.
    raw = meta.get("actifs_concernes", []) or []
    if isinstance(raw, str):          # le LLM a renvoyé une chaîne au lieu d'une liste
        raw = [raw]
    concerned_active = [str(x).lower() for x in raw]
    matched = []
    for symbol, keywords in alias_map.items():
        if any(kw in actif for kw in keywords for actif in concerned_active):
            matched.append(symbol)
    return matched


def main() -> None:
    alias_map = build_alias_map()
    client = MongoClient(MONGO_URI)
    collection = client["bourse"]["actualites"]

    # On ne ré-enrichit pas ce qui l'a déjà été (idempotent / reprenable).
    articles = list(
        collection.find({"metadata": {"$exists": False}}).limit(SAMPLE_SIZE)
    )
    print(f"{len(articles)} article(s) à enrichir...\n")

    for i, art in enumerate(articles, 1):
        meta = llm.enrich(art.get("title", ""), art.get("summary", ""))
        if not meta:
            print(f"  [{i}/{len(articles)}] JSON non exploitable, ignoré")
            continue

        symbols = match_symbols(meta, alias_map)
        collection.update_one(
            {"_id": art["_id"]},
            {"$set": {
                "metadata": meta,
                "symboles_associes": symbols,
                "enriched_at": datetime.now(timezone.utc),
            }},
        )
        print(f"  [{i}/{len(articles)}] {meta.get('type_evenement','?'):14} "
              f"-> {symbols}  | {art.get('title','')[:50]}")

    client.close()
    print("\nEnrichissement terminé.")


if __name__ == "__main__":
    main()
