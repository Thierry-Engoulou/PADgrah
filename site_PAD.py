import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import sqlite3
import numpy as np
from datetime import datetime

# --- Connexion SQLite ---
conn = sqlite3.connect("demandes.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS demandes (
    id TEXT PRIMARY KEY,
    nom TEXT,
    structure TEXT,
    email TEXT,
    raison TEXT,
    statut TEXT,
    token TEXT,
    timestamp REAL
)
''')
conn.commit()

# --- Page Streamlit ---
st.set_page_config(page_title="Météo Douala", layout="wide")
st.title("Visualisation des données 📊📈")

# --- API ---
API_URL = "https://data-real-time-2.onrender.com/donnees?limit=5000000"  # limite réaliste

try:
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    data = response.json()
    if not data:
        st.warning("L'API a renvoyé une réponse vide.")
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(data)
except requests.exceptions.RequestException as e:
    st.error(f"Erreur réseau : {e}")
    df = pd.DataFrame()
except ValueError as e:
    st.error(f"Impossible de décoder le JSON : {e}")
    df = pd.DataFrame()

# --- Nettoyage des données ---
if not df.empty:
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df = df.sort_values("DateTime", ascending=False)

    # Colonnes numériques
    params = ["TIDE HEIGHT", "WIND SPEED", "WIND DIR", "AIR PRESSURE", 
              "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]
    for p in params:
        df[p] = pd.to_numeric(df[p], errors='coerce')

    # Colonnes booléennes
    bool_columns = ["TIDE_HIGH", "TIDE_LOW"]
    for col in bool_columns:
        df[col] = df[col].replace({False: np.nan, True: 1})

    # --- Filtre par date ---
    st.sidebar.header("🗕️ Filtrer par date")
    min_date = df["DateTime"].min().date()
    max_date = df["DateTime"].max().date()
    start_date, end_date = st.sidebar.date_input("Plage de dates", [min_date, max_date])
    df = df[(df["DateTime"].dt.date >= start_date) & (df["DateTime"].dt.date <= end_date)]

    # Slider pour lissage
    window_size = st.sidebar.slider("Taille fenêtre lissage", 1, 21, 5, step=2)

    # --- Onglets comparaison ---
    tab1, tab2 = st.tabs(["🗓️ 30 derniers jours", "🗕️ Période personnalisée"])

    with tab1:
        df_last_30 = df[df["DateTime"] >= (df["DateTime"].max() - pd.Timedelta(days=30))].copy()
        for p in params:
            df_plot = df_last_30.dropna(subset=[p])
            if not df_plot.empty:
                df_plot[p+'_smoothed'] = df_plot[p].rolling(window=window_size, min_periods=1, center=True).mean()
                fig = px.line(df_plot, x="DateTime", y=p+'_smoothed', color="Station",
                              title=f"Comparaison – {p} (30 derniers jours)")
                if p == "TIDE HEIGHT":
                    fig.update_yaxes(range=[0, df_plot[p+'_smoothed'].max()+0.5])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"Aucune donnée disponible pour {p} (30 derniers jours)")

    with tab2:
        start_custom, end_custom = st.date_input("Période à comparer", [min_date, max_date], key="compare_range")
        df_custom = df[(df["DateTime"].dt.date >= start_custom) & (df["DateTime"].dt.date <= end_custom)].copy()
        for p in params:
            df_plot = df_custom.dropna(subset=[p])
            if not df_plot.empty:
                df_plot[p+'_smoothed'] = df_plot[p].rolling(window=window_size, min_periods=1, center=True).mean()
                fig = px.line(df_plot, x="DateTime", y=p+'_smoothed', color="Station",
                              title=f"Comparaison – {p} ({start_custom} → {end_custom})")
                if p == "TIDE HEIGHT":
                    fig.update_yaxes(range=[0, df_plot[p+'_smoothed'].max()+0.5])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"Aucune donnée disponible pour {p} dans cette période")

    # --- Carte Windy ---
    st.subheader("🌐 Carte météo animée – Windy")
    st.components.v1.html('''
    <iframe width="100%" height="450" src="https://embed.windy.com/embed2.html?lat=4.05&lon=9.68&zoom=9&type=wind" frameborder="0"></iframe>
    ''', height=450)

