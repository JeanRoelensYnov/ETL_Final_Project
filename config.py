import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(
            f"Variable d'environnement '{key}' manquante. "
            f"Copie .env.example vers .env et renseigne-la."
        )
    return val


# --- Kafka ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# --- MySQL (clients Python : PyMySQL) ---
MYSQL_CONF = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3307")),
    "user": os.getenv("MYSQL_USER", "etl"),
    "password": _require("MYSQL_PASSWORD"),       # obligatoire -> doit venir de .env
    "database": os.getenv("MYSQL_DB", "bourse"),
}

# --- MySQL vu par Spark (JDBC : même base, autre format de connexion) ---
JDBC_URL = f"jdbc:mysql://{MYSQL_CONF['host']}:{MYSQL_CONF['port']}/{MYSQL_CONF['database']}"
JDBC_PROPS = {
    "user": MYSQL_CONF["user"],
    "password": MYSQL_CONF["password"],
    "driver": "com.mysql.cj.jdbc.Driver",
}

# --- MongoDB ---
MONGO_URI = (
    f"mongodb://{os.getenv('MONGO_USER', 'root')}:{_require('MONGO_PASSWORD')}"
    f"@{os.getenv('MONGO_HOST', 'localhost')}:{os.getenv('MONGO_PORT', '27017')}"
    f"/?authSource=admin"
)
