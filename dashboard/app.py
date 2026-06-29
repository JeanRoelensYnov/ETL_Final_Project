import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient
import pymysql

# Rend config.py (à la racine) importable depuis ce sous-dossier.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MONGO_URI, MYSQL_CONF

st.set_page_config(page_title="Marchés & Événements", layout="wide")



@st.cache_data(ttl=60)
def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Exécute une requête MySQL et renvoie un DataFrame."""
    conn = pymysql.connect(**MYSQL_CONF)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=60)
def load_news() -> pd.DataFrame:
    """Charge les actualités enrichies depuis MongoDB."""
    client = MongoClient(MONGO_URI)
    docs = list(client["bourse"]["actualites"].find({"metadata": {"$exists": True}}))
    client.close()
    rows = []
    for d in docs:
        meta = d.get("metadata", {})
        rows.append({
            "title": d.get("title", ""),
            "categorie": d.get("categorie", ""),
            "type_evenement": meta.get("type_evenement", ""),
            "impact_attendu": meta.get("impact_attendu", ""),
            "localisation": meta.get("localisation", ""),
            "symboles": ", ".join(d.get("symboles_associes", []) or []),
            "link": d.get("link", ""),
        })
    return pd.DataFrame(rows)



st.title("📊 Plateforme d'Analyse des Marchés & Événements")

tab_marche, tab_actif, tab_news = st.tabs(
    ["📈 Vue marché", "🔍 Détail actif", "📰 Actualités enrichies"]
)


with tab_marche:
    st.subheader("Derniers cours par actif")
    df = query_df("""
        SELECT a.symbol, a.name, a.type, c.close, c.ts
        FROM actif a
        JOIN cours_bourse c ON c.symbol = a.symbol
        WHERE c.ts = (SELECT MAX(ts) FROM cours_bourse c2 WHERE c2.symbol = a.symbol)
        ORDER BY a.type, a.symbol
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)


with tab_actif:
    actifs = query_df("SELECT symbol, name FROM actif ORDER BY symbol")
    choix = st.selectbox(
        "Choisir un actif",
        actifs["symbol"],
        format_func=lambda s: f"{s} — {actifs.set_index('symbol').loc[s, 'name']}",
    )

    prix = query_df(
        "SELECT trade_date, close, volume FROM cours_journalier "
        "WHERE symbol=%s ORDER BY trade_date", (choix,),
    )
    mes = query_df(
        "SELECT trade_date, sma_20, sma_50, volatility_30, trend "
        "FROM mesures WHERE symbol=%s ORDER BY trade_date", (choix,),
    )

    if prix.empty:
        st.info("Pas de données pour cet actif.")
    else:
        # tendance courante (dernière ligne de mesures)
        if not mes.empty:
            trend = mes.iloc[-1]["trend"]
            emoji = "🟢" if trend == "haussiere" else "🔴"
            st.metric("Tendance actuelle", f"{emoji} {trend}")

        # Prix + moyennes mobiles
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=prix["trade_date"], y=prix["close"].astype(float),
                                 name="Cours", line=dict(width=2)))
        if not mes.empty:
            fig.add_trace(go.Scatter(x=mes["trade_date"], y=mes["sma_20"].astype(float),
                                     name="SMA 20", line=dict(dash="dot")))
            fig.add_trace(go.Scatter(x=mes["trade_date"], y=mes["sma_50"].astype(float),
                                     name="SMA 50", line=dict(dash="dash")))
        fig.update_layout(title=f"{choix} — prix & tendance (3 mois)", height=400)
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            if not mes.empty:
                figv = go.Figure(go.Scatter(
                    x=mes["trade_date"], y=mes["volatility_30"].astype(float),
                    fill="tozeroy", name="Volatilité 30j"))
                figv.update_layout(title="Volatilité (30j)", height=300)
                st.plotly_chart(figv, use_container_width=True)
        with col2:
            figvol = go.Figure(go.Bar(x=prix["trade_date"], y=prix["volume"]))
            figvol.update_layout(title="Volume", height=300)
            st.plotly_chart(figvol, use_container_width=True)


with tab_news:
    st.subheader("Actualités enrichies par le LLM")
    news = load_news()
    if news.empty:
        st.info("Aucune actualité enrichie pour l'instant "
                "(le batch d'enrichissement est peut-être encore en cours).")
    else:
        c1, c2 = st.columns(2)
        types = c1.multiselect("Type d'événement", sorted(news["type_evenement"].unique()))
        impacts = c2.multiselect("Impact attendu", sorted(news["impact_attendu"].unique()))
        filt = news
        if types:
            filt = filt[filt["type_evenement"].isin(types)]
        if impacts:
            filt = filt[filt["impact_attendu"].isin(impacts)]
        st.caption(f"{len(filt)} article(s)")
        st.dataframe(filt, use_container_width=True, hide_index=True,
                     column_config={"link": st.column_config.LinkColumn("lien")})
