SYMBOLS = {
    "actions": ["AAPL", "MSFT", "NVDA", "GOOGL", "TSLA"],
    "crypto":  ["BTC-USD", "ETH-USD"],
    "indices": ["^GSPC", "^FCHI", "^GDAXI"],   # S&P 500, CAC 40, DAX
}

# Liste à plat de tous les symboles.
ALL_SYMBOLS = [s for groupe in SYMBOLS.values() for s in groupe]
