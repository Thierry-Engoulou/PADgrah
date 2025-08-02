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
st.title("🌦️ Tableau de bord MeteoMarine – Port Autonome de Douala")

# Chargement données
API_URL = "https://data-real-time-2.onrender.com/donnees?limit=50000000000"
data = requests.get(API_URL).json()
df = pd.DataFrame(data)

df["DateTime"] = pd.to_datetime(df["DateTime"])
df = df.sort_values("DateTime", ascending=False)

# --- Filtre date ---
st.sidebar.header("📅 Filtrer par date")
min_date = df["DateTime"].min().date()
max_date = df["DateTime"].max().date()
start_date, end_date = st.sidebar.date_input("Plage de dates", [min_date, max_date])
df = df[(df["DateTime"].dt.date >= start_date) & (df["DateTime"].dt.date <= end_date)]

# --- Aperçu météo ---
# --- Carte interactive ---
# --- Graphiques
st.subheader("📈 Graphique par station et paramètre")

station_selected = st.selectbox("Station", df["Station"].unique())
params = ["AIR TEMPERATURE", "HUMIDITY", "WIND SPEED", "AIR PRESSURE"]
if "TIDE HEIGHT" in df.columns:
    params.append("TIDE HEIGHT")
if "SURGE" in df.columns:
    params.append("SURGE")

param = st.selectbox("Paramètre", params)
df_station = df[df["Station"] == station_selected].copy()
df_station[param] = pd.to_numeric(df_station[param], errors='coerce')
df_station = df_station.dropna(subset=[param])
if param == "TIDE HEIGHT":
    df_station = df_station[df_station[param] >= 0.3]
fig = px.line(df_station, x="DateTime", y=param, title=f"{param} à {station_selected}")
st.plotly_chart(fig, use_container_width=True)

# === 📊 Comparaison entre stations ===
st.subheader("📊 Comparaison multistation")

# Copie pour conversion numérique
df_numeric = df.copy()
for p in params:
    df_numeric[p] = pd.to_numeric(df_numeric[p], errors='coerce')

for p in params:
    df_plot = df_numeric.dropna(subset=[p])
    df_plot = df_plot[(df_plot["DateTime"].dt.date >= start_date) & (df_plot["DateTime"].dt.date <= end_date)]

    fig = px.line(df_plot, x="DateTime", y=p, color="Station", title=f"Comparaison – {p}")
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

# --- Demande utilisateur