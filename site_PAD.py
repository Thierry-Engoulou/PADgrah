import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import folium
from streamlit_folium import st_folium
from datetime import datetime
import sqlite3
import uuid
import time
from folium.features import DivIcon
from branca.element import MacroElement
from jinja2 import Template
import math

# Connexion à la base SQLite
conn = sqlite3.connect("demandes.db", check_same_thread=False)
cursor = conn.cursor()

# Création table des demandes
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

st.set_page_config(page_title="Météo Douala", layout="wide")
st.title("📊📈📥 Visualisation + exportation de données météo des stations du  – Port Autonome de Douala")

# Chargement données
API_URL = "https://data-real-time-2.onrender.com/donnees?limit=50000000000"
data = requests.get(API_URL).json()
df = pd.DataFrame(data)

df["DateTime"] = pd.to_datetime(df["DateTime"])
df = df.sort_values("DateTime", ascending=False)

# --- Filtre date ---
st.sidebar.header("🗕️ Filtrer par date")
min_date = df["DateTime"].min().date()
max_date = df["DateTime"].max().date()
start_date, end_date = st.sidebar.date_input("Plage de dates", [min_date, max_date])
df = df[(df["DateTime"].dt.date >= start_date) & (df["DateTime"].dt.date <= end_date)]

# === 📊 Comparaison entre stations ===
st.subheader("📊 Comparaison multistation")

# 🔧 Définir les paramètres numériques à comparer
params = ["TIDE HEIGHT", "WIND SPEED", "WIND DIR", "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]

# Préparation des données numériques
df_numeric = df.copy()
for p in params:
    df_numeric[p] = pd.to_numeric(df_numeric[p], errors='coerce')

tab1, tab2 = st.tabs(["🗓️ 30 derniers jours", "🗕️ Période personnalisée"])

# 🔹 Onglet : 30 derniers jours
with tab1:
    df_last_30 = df_numeric[df_numeric["DateTime"] >= (df_numeric["DateTime"].max() - pd.Timedelta(days=30))].copy()
    for p in params:
        df_plot = df_last_30.dropna(subset=[p])
        fig = px.line(df_plot, x="DateTime", y=p, color="Station", title=f"Comparaison – {p} (30 derniers jours)")
        if p == "TIDE HEIGHT":
            max_val = df_plot[p].max()
            if pd.notnull(max_val):
                fig.update_yaxes(range=[0, max_val + 0.5])
        st.plotly_chart(fig, use_container_width=True)

# 🔹 Onglet : Période personnalisée
with tab2:
    start_custom, end_custom = st.date_input("Période à comparer", [min_date, max_date], key="compare_range")
    df_custom = df_numeric[
        (df_numeric["DateTime"].dt.date >= start_custom) & (df_numeric["DateTime"].dt.date <= end_custom)
    ].copy()
    for p in params:
        df_plot = df_custom.dropna(subset=[p])
        fig = px.line(df_plot, x="DateTime", y=p, color="Station", title=f"Comparaison – {p} ({start_custom} → {end_custom})")
        if p == "TIDE HEIGHT":
            max_val = df_plot[p].max()
            if pd.notnull(max_val):
                fig.update_yaxes(range=[0, max_val + 0.5])
        st.plotly_chart(fig, use_container_width=True)

# --- Carte météo Windy
st.subheader("🌐 Carte météo animée – Windy")
st.components.v1.html('''
<iframe width="100%" height="450" src="https://embed.windy.com/embed2.html?lat=4.05&lon=9.68&zoom=9&type=wind" frameborder="0"></iframe>
''', height=450)
