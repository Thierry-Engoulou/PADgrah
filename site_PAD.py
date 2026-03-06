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

API_URL = "https://data-real-time-2.onrender.com/donnees"

# --- Fonction récupération par batch ---
@st.cache_data(ttl=600)
def load_all_data(batch_limit=2000):

    all_data = []
    offset = 0

    progress = st.progress(0)
    status = st.empty()

    while True:

        url = f"{API_URL}?limit={batch_limit}&offset={offset}"

        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            st.error(f"Erreur API : {e}")
            break

        if not data:
            break

        all_data.extend(data)
        offset += batch_limit

        status.text(f"Chargement {len(all_data)} lignes...")
        progress.progress(min(len(all_data)/200000,1.0))

        if len(data) < batch_limit:
            break

    progress.empty()
    status.empty()

    if not all_data:
        return pd.DataFrame()

    return pd.DataFrame(all_data)


# --- Bouton chargement ---
if "df" not in st.session_state:

    st.info("Clique sur le bouton pour charger les données.")

    if st.button("🚀 Charger les données"):
        st.session_state.df = load_all_data()

    st.stop()


df = st.session_state.df


# --- Nettoyage données ---
df["DateTime"] = pd.to_datetime(df["DateTime"])
df = df.sort_values("DateTime", ascending=False)

params = [
"TIDE HEIGHT",
"WIND SPEED",
"WIND DIR",
"AIR PRESSURE",
"AIR TEMPERATURE",
"DEWPOINT",
"HUMIDITY"
]

for p in params:
    df[p] = pd.to_numeric(df[p], errors="coerce")

bool_columns = ["TIDE_HIGH", "TIDE_LOW"]

for col in bool_columns:
    df[col] = df[col].replace({False: np.nan, True: 1})


# --- Filtre date ---
st.sidebar.header("🗓️ Filtrer par date")

min_date = df["DateTime"].min().date()
max_date = df["DateTime"].max().date()

start_date, end_date = st.sidebar.date_input(
"Plage de dates",
[min_date, max_date]
)

df = df[
(df["DateTime"].dt.date >= start_date) &
(df["DateTime"].dt.date <= end_date)
]


# --- Slider lissage ---
window_size = st.sidebar.slider(
"Taille fenêtre lissage",
1,21,5,step=2
)


# --- Fonction échantillonnage ---
def sample_data(df,max_points=5000):

    if len(df) <= max_points:
        return df

    step = len(df)//max_points

    return df.iloc[::step]


# --- Onglets ---
tab1, tab2 = st.tabs([
"🗓️ 30 derniers jours",
"📅 Période personnalisée"
])


# --- 30 jours ---
with tab1:

    df_last_30 = df[
        df["DateTime"] >=
        (df["DateTime"].max()-pd.Timedelta(days=30))
    ].copy()

    for p in params:

        df_plot = df_last_30.dropna(subset=[p])

        if not df_plot.empty:

            df_plot[p+"_smooth"] = df_plot[p].rolling(
                window=window_size,
                min_periods=1,
                center=True
            ).mean()

            df_plot = sample_data(df_plot)

            fig = px.line(
                df_plot,
                x="DateTime",
                y=p+"_smooth",
                color="Station",
                title=f"{p} (30 derniers jours)"
            )

            st.plotly_chart(fig,use_container_width=True)

        else:
            st.info(f"Aucune donnée pour {p}")


# --- période personnalisée ---
with tab2:

    start_custom,end_custom = st.date_input(
        "Période",
        [min_date,max_date],
        key="custom"
    )

    df_custom = df[
        (df["DateTime"].dt.date >= start_custom) &
        (df["DateTime"].dt.date <= end_custom)
    ].copy()

    for p in params:

        df_plot = df_custom.dropna(subset=[p])

        if not df_plot.empty:

            df_plot[p+"_smooth"] = df_plot[p].rolling(
                window=window_size,
                min_periods=1,
                center=True
            ).mean()

            df_plot = sample_data(df_plot)

            fig = px.line(
                df_plot,
                x="DateTime",
                y=p+"_smooth",
                color="Station",
                title=f"{p} ({start_custom} → {end_custom})"
            )

            st.plotly_chart(fig,use_container_width=True)

        else:
            st.info(f"Aucune donnée pour {p}")


# --- Carte Windy ---
st.subheader("🌍 Carte météo – Windy")

st.components.v1.html(
"""
<iframe width="100%" height="450"
src="https://embed.windy.com/embed2.html?lat=4.05&lon=9.68&zoom=9&type=wind"
frameborder="0"></iframe>
""",
height=450
)
