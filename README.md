# Plateforme d'Analyse des Marchés Boursiers et des Événements

**Stack** : NiFi · Kafka · Spark · MySQL · MongoDB · LLM (Qwen2.5) · Streamlit — orchestrés par Docker + conda.

```
NiFi (60s) ─► producer.py ─► Kafka ─┬─► Spark ─► MySQL (cours, mesures)
                                    └─► consumer ─► MongoDB (actualités + LLM)
                                                        │
                                                  Dashboard Streamlit
```

## Prérequis

- **Docker Desktop** lancé
- **conda** avec l'environnement du projet
- **JDK 17** (pour NiFi) — déjà requis par `start-nifi.ps1`
- Fichier **`.env`** à la racine : copier `.env.example` → `.env`
- **NiFi** available [here](https://nifi.apache.org/download/)

```powershell
conda create -n etl-bourse python=3.12 -y
conda activate etl-bourse
pip install -r requirements.txt
```

## Démarrage rapide

Les données persistent dans les volumes Docker. Pour relancer et visualiser :

```powershell
conda activate etl-bourse
docker compose up -d                 # Kafka + MySQL + MongoDB
streamlit run dashboard/app.py       # dashboard sur http://localhost:8501
```

## Démarrage complet (pipeline live + reconstruction)

Pour lancer l'ingestion continue (NiFi), le traitement Spark, l'enrichissement LLM,
ou reconstruire depuis une base vide, importer [bourse_ingestion.xml](nifi/bourse_ingestion.xml) dans nifi et changer les configuration
des blocks processus pour correspondre avec les path de votre machine locale.

Dans un usage industriel NiFi devrait être mis dans le docker-compose avec un auto import du .xml pour une meilleur reproductibilité.

## Structure du projet

| Dossier | Contenu |
|---------|---------|
| `ingestion/` | collecte (yfinance + RSS), consumers Kafka, chargement dimension/historique |
| `streaming/` | jobs Spark (Kafka→MySQL, calcul des mesures) |
| `enrichment/` | enrichissement LLM des actualités |
| `dashboard/` | application Streamlit |
| `db/mysql/` | schémas SQL (exécutés au 1er démarrage de MySQL) |
| `nifi/` | template du flow d'orchestration (réimportable) |
| `docs/` | guide de démarrage + journal d'architecture |

