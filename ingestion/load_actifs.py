import os
import sys

import pymysql
import yfinance as yf

# Rend config.py (à la racine du projet) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MYSQL_CONF
from symbols import ALL_SYMBOLS

# UPSERT : insère, ou met à jour si le symbole existe déjà.
UPSERT_SQL = """
    INSERT INTO actif (symbol, name, type, sector, industry, country, currency, exchange)
    VALUES (%(symbol)s, %(name)s, %(type)s, %(sector)s, %(industry)s,
            %(country)s, %(currency)s, %(exchange)s)
    ON DUPLICATE KEY UPDATE
        name=VALUES(name), type=VALUES(type), sector=VALUES(sector),
        industry=VALUES(industry), country=VALUES(country),
        currency=VALUES(currency), exchange=VALUES(exchange)
"""


def info_to_row(symbol: str, info: dict) -> dict:
    return {
        "symbol": symbol,
        "name": info.get("shortName",""),
        "type": info.get("quoteType",""),
        "sector":info.get("sector"),
        "industry":info.get("industry"),
        "country":info.get("country"),
        "currency":info.get("currency",""),
        "exchange":info.get("exchange","")
    }

def main() -> None:
    conn = pymysql.connect(**MYSQL_CONF)
    cursor = conn.cursor()

    for symbol in ALL_SYMBOLS:
        info = yf.Ticker(symbol).info
        row = info_to_row(symbol, info)
        cursor.execute(UPSERT_SQL, row)
        print(f"  {row['symbol']:10} {row['type']:16} {row['name']}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n{len(ALL_SYMBOLS)} actif(s) chargé(s).")


if __name__ == "__main__":
    main()
