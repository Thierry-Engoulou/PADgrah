import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import sqlite3
import uuid
import numpy as np
from datetime import datetime

# --- Connexion SQLite et création table demandes ---
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

# --- Chargement données depuis API ---
API_URL = "https://data-real-time-2.onrender.com/donnees?limit=50000000000"
response = requests.get(API_URL)
data = response.json()
df = pd.DataFrame(data)

# --- Nettoyage et conversion des données ---
def clean_data(df, params, bool_columns=None):
    for p in params:
        df[p] = pd.to_numeric(df[p], errors='coerce')
    if bool_columns:
        for col in bool_columns:
            df[col] = df[col].replace({False: np.nan, True: 1})
    return df

params = ["TIDE HEIGHT", "WIND SPEED", "WIND DIR", "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]
bool_columns = ["TIDE_HIGH", "TIDE_LOW"]
df = clean_data(df, params, bool_columns)

df["DateTime"] = pd.to_datetime(df["DateTime"])
df = df.sort_values("DateTime", ascending=False)
min_date = df["DateTime"].min().date()
max_date = df["DateTime"].max().date()

# --- Streamlit page config ---
st.set_page_config(page_title="Météo Douala", layout="wide")
st.title("🌤️ Visualisation météo & gestion des demandes")

# --- Sidebar menu ---
menu = st.sidebar.selectbox("Choisir la section", ["Visualisation météo", "Gestion des demandes"])
start_date, end_date = st.sidebar.date_input("Plage de dates", [min_date, max_date])
window_size = st.sidebar.slider("Taille fenêtre lissage (rolling mean)", 1, 21, 5, 2)

# --- Filtrage date ---
df = df[(df["DateTime"].dt.date >= start_date) & (df["DateTime"].dt.date <= end_date)]

# === Section météo ===
if menu == "Visualisation météo":
    st.subheader("📊 Comparaison multistation")

    tab1, tab2 = st.tabs(["🗓️ 30 derniers jours", "🗕️ Période personnalisée"])

    # Onglet 30 derniers jours
    with tab1:
        df_last_30 = df[df["DateTime"] >= (df["DateTime"].max() - pd.Timedelta(days=30))].copy()
        for p in params:
            df_plot = df_last_30.dropna(subset=[p])
            if not df_plot.empty:
                df_plot[p + "_smoothed"] = df_plot[p].rolling(window=window_size, min_periods=1, center=True).mean()
                fig = px.line(df_plot, x="DateTime", y=p + "_smoothed", color="Station",
                              title=f"Comparaison – {p} (30 derniers jours)")
                if p == "TIDE HEIGHT":
                    max_val = df_plot[p + "_smoothed"].max()
                    if pd.notnull(max_val):
                        fig.update_yaxes(range=[0, max_val + 0.5])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"Aucune donnée disponible pour {p} dans les 30 derniers jours.")

    # Onglet période personnalisée
    with tab2:
        start_custom, end_custom = st.date_input("Période à comparer", [min_date, max_date], key="compare_range")
        df_custom = df[(df["DateTime"].dt.date >= start_custom) & (df["DateTime"].dt.date <= end_custom)].copy()
        for p in params:
            df_plot = df_custom.dropna(subset=[p])
            if not df_plot.empty:
                df_plot[p + "_smoothed"] = df_plot[p].rolling(window=window_size, min_periods=1, center=True).mean()
                fig = px.line(df_plot, x="DateTime", y=p + "_smoothed", color="Station",
                              title=f"Comparaison – {p} ({start_custom} → {end_custom})")
                if p == "TIDE HEIGHT":
                    max_val = df_plot[p + "_smoothed"].max()
                    if pd.notnull(max_val):
                        fig.update_yaxes(range=[0, max_val + 0.5])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"Aucune donnée disponible pour {p} dans cette période.")

    # Carte Windy
    st.subheader("🌐 Carte météo animée – Windy")
    st.components.v1.html('''
    <iframe width="100%" height="450" src="https://embed.windy.com/embed2.html?lat=4.05&lon=9.68&zoom=9&type=wind" frameborder="0"></iframe>
    ''', height=450)

# === Section gestion des demandes ===
elif menu == "Gestion des demandes":
    st.subheader("💼 Gestion des demandes utilisateurs")

    with st.form("form_demandes", clear_on_submit=True):
        nom = st.text_input("Nom")
        structure = st.text_input("Structure")
        email = st.text_input("Email")
        raison = st.text_area("Raison de la demande")
        submitted = st.form_submit_button("Envoyer la demande")

        if submitted:
            if nom and email and raison:
                id_unique = str(uuid.uuid4())
                token = str(uuid.uuid4())
                timestamp = datetime.now().timestamp()
                statut = "En attente"
                cursor.execute('''
                    INSERT INTO demandes (id, nom, structure, email, raison, statut, token, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (id_unique, nom, structure, email, raison, statut, token, timestamp))
                conn.commit()
                st.success("Demande enregistrée avec succès !")
            else:
                st.error("Merci de remplir au minimum le nom, l'email et la raison.")

    # Affichage demandes existantes
    st.markdown("### Liste des demandes")
    cursor.execute("SELECT id, nom, structure, email, raison, statut, timestamp FROM demandes ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    if rows:
        df_demandes = pd.DataFrame(rows, columns=["ID", "Nom", "Structure", "Email", "Raison", "Statut", "Timestamp"])
        df_demandes["Date"] = pd.to_datetime(df_demandes["Timestamp"], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(df_demandes.drop(columns=["Timestamp"]))
    else:
        st.info("Aucune demande enregistrée pour le moment.")
