import os
import sys

import pymysql
import yfinance as yf

# Rend config.py (à la racine du projet) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MYSQL_CONF
from symbols import ALL_SYMBOLS

PERIOD = "3mo"

UPSERT_SQL = """
    INSERT INTO cours_journalier (symbol, trade_date, open, high, low, close, volume)
    VALUES (%(symbol)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open), high=VALUES(high), low=VALUES(low),
        close=VALUES(close), volume=VALUES(volume)
"""


def history_rows(symbol: str) -> list[dict]:
    """Récupère l'historique quotidien d'un symbole en lignes prêtes pour l'INSERT."""
    df = yf.Ticker(symbol).history(period=PERIOD, interval="1d")
    rows = []
    for idx, r in df.iterrows():
        rows.append({
            "symbol": symbol,
            "trade_date": idx.date(),      
            "open": float(r["Open"]),
            "high": float(r["High"]),
            "low": float(r["Low"]),
            "close": float(r["Close"]),
            "volume": int(r["Volume"]),
        })
    return rows


def main() -> None:
    conn = pymysql.connect(**MYSQL_CONF)
    cursor = conn.cursor()

    total = 0
    for symbol in ALL_SYMBOLS:
        rows = history_rows(symbol)
        if rows:
            cursor.executemany(UPSERT_SQL, rows)
            conn.commit()
        total += len(rows)
        print(f"  {symbol:10} {len(rows):>3} jours")

    cursor.close()
    conn.close()
    print(f"\n{total} lignes journalières chargées.")


if __name__ == "__main__":
    main()
