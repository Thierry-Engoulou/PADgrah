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
st.title(" Visualisation + exportation de données météo des stations du  – Port Autonome de Douala")

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

# ===  Comparaison entre stations ===
st.subheader(" Comparaison multistation")

#  Définir les paramètres numériques à comparer
params = ["TIDE HEIGHT", "WIND SPEED", "WIND DIR", "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]

# Préparation des données numériques
df_numeric = df.copy()
for p in params:
    df_numeric[p] = pd.to_numeric(df_numeric[p], errors='coerce')

tab1, tab2 = st.tabs([" 30 derniers jours", "🗕️ Période personnalisée"])

#  Onglet : 30 derniers jours
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

#  Onglet : Période personnalisée
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
st.subheader(" Carte météo animée – Windy")
st.components.v1.html('''
<iframe width="100%" height="450" src="https://embed.windy.com/embed2.html?lat=4.05&lon=9.68&zoom=9&type=wind" frameborder="0"></iframe>
''', height=450)

# --- Demande utilisateur
st.subheader(" Demande de téléchargement des données météo")

with st.form("form_demande"):
    nom = st.text_input("Votre nom")
    structure = st.text_input("Structure")
    email = st.text_input("Votre email")
    raison = st.text_area("Raison de la demande")
    submit = st.form_submit_button("Envoyer la demande")

if submit:
    if not nom or not structure or not email or not raison:
        st.error("Tous les champs sont requis.")
    else:
        demande_id = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO demandes (id, nom, structure, email, raison, statut, token, timestamp)
            VALUES (?, ?, ?, ?, ?, 'en attente', NULL, NULL)
        ''', (demande_id, nom, structure, email, raison))
        conn.commit()
        st.success(" Demande envoyée. En attente de validation par l’administrateur.")

# --- Vérification des droits de téléchargement
cursor.execute('SELECT * FROM demandes WHERE email = ? AND statut = "acceptée"', (email,))
row = cursor.fetchone()
user_demande = None
if row:
    _, _, _, _, _, _, _, timestamp = row
    if timestamp and time.time() - timestamp <= 60:
        user_demande = row
    else:
        cursor.execute("UPDATE demandes SET statut = 'expirée' WHERE email = ?", (email,))
        conn.commit()

if user_demande:
    st.success(" Votre demande est acceptée. Vous avez 60 secondes pour télécharger.")
    export_cols = ["Station", "Latitude", "Longitude", "DateTime", "TIDE HEIGHT", "WIND SPEED", "WIND DIR",
                   "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]
    df_export = df[export_cols]
    csv = df_export.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=" Télécharger les données météo",
        data=csv,
        file_name="MeteoMarinePAD.csv",
        mime="text/csv"
    )
else:
    if email:
        cursor.execute('SELECT * FROM demandes WHERE email = ? AND statut = "expirée"', (email,))
        if cursor.fetchone():
            st.warning(" Le lien a expiré. Veuillez refaire une demande.")

# --- Notification publique si des demandes sont en attente
cursor.execute("SELECT COUNT(*) FROM demandes WHERE statut = 'en attente'")
nb_attente = cursor.fetchone()[0]

if nb_attente > 0:
    st.sidebar.warning(f" {nb_attente} demande(s) en attente de validation.")

# --- Interface admin
st.sidebar.header(" Admin")
admin_password = st.sidebar.text_input("Mot de passe admin", type="password")

if admin_password == "SHy@2025":
    st.sidebar.success("Accès admin autorisé")
    st.sidebar.markdown("###  Demandes en attente")
    cursor.execute("SELECT * FROM demandes WHERE statut = 'en attente'")
    demandes_attente = cursor.fetchall()
    for d in demandes_attente:
        demande_id, nom, structure, email, raison, _, _, _ = d
        st.sidebar.markdown(f"**{nom} ({email})**")
        st.sidebar.markdown(f"Structure : {structure}")
        st.sidebar.markdown(f"Raison : {raison}")
        col1, col2 = st.sidebar.columns(2)
        if col1.button(f" Accepter {demande_id}", key=f"acc_{demande_id}"):
            token = str(uuid.uuid4())
            cursor.execute("UPDATE demandes SET statut='acceptée', token=?, timestamp=? WHERE id=?",
                           (token, time.time(), demande_id))
            conn.commit()
            st.sidebar.success(f"Acceptée pour {nom}")
        if col2.button(f" Refuser {demande_id}", key=f"ref_{demande_id}"):
            cursor.execute("UPDATE demandes SET statut='refusée', timestamp=? WHERE id=?",
                           (time.time(), demande_id))
            conn.commit()
            st.sidebar.warning(f"Refusée pour {nom}")
    st.sidebar.markdown("---")
    st.sidebar.markdown("###  Historique des décisions")
    cursor.execute("SELECT * FROM demandes WHERE statut IN ('acceptée', 'refusée')")
    demandes_traitees = cursor.fetchall()
    for d in demandes_traitees:
        _, nom, structure, email, raison, statut, _, ts = d
        couleur = "" if statut == "acceptée" else ""
        heure = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "Inconnu"
        st.sidebar.markdown(f"""
        {couleur} **{nom}**  
         {email}  
         {structure}  
         {raison}  
         {heure}
        """)
    cursor.execute("SELECT nom, email, structure, raison, statut, timestamp FROM demandes")
    export_data = cursor.fetchall()
    df_export = pd.DataFrame(export_data, columns=["nom", "email", "structure", "raison", "statut", "timestamp"])
    df_export["Horodatage"] = df_export["timestamp"].apply(
        lambda ts: datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "")
    df_export = df_export.drop(columns=["timestamp"])
    st.sidebar.download_button(
        label=" Exporter l’historique",
        data=df_export.to_csv(index=False).encode("utf-8"),
        file_name="historique_acces.csv",
        mime="text/csv"
    )
elif admin_password != "":
    st.sidebar.error("Mot de passe incorrect.")