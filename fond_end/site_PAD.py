import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import folium
from streamlit_folium import st_folium
from datetime import datetime
from alerte import get_weather_icon

st.set_page_config(page_title="Météo Douala", layout="wide")
st.title("🌦️ Tableau de bord météo – Douala")

API_URL = "https://data-real-time-2.onrender.com/donnees?limit=5000"
data = requests.get(API_URL).json()
df = pd.DataFrame(data)

df["DateTime"] = pd.to_datetime(df["DateTime"])
df = df.sort_values("DateTime", ascending=False)

# === 📅 Filtre de date ===
st.sidebar.header("📅 Filtrer par date")
min_date = df["DateTime"].min().date()
max_date = df["DateTime"].max().date()
start_date, end_date = st.sidebar.date_input("Plage de dates", [min_date, max_date])
df = df[(df["DateTime"].dt.date >= start_date) & (df["DateTime"].dt.date <= end_date)]

# === 🔟 Affichage des dernières données ===
st.subheader("🔟 Dernières observations météo")
cols_display = ["DateTime", "Station", "AIR TEMPERATURE", "HUMIDITY", "WIND SPEED", "AIR PRESSURE"]
if "TIDE HEIGHT" in df.columns:
    cols_display.append("TIDE HEIGHT")
if "SURGE" in df.columns:
    cols_display.append("SURGE")

col1, col2 = st.columns([1, 2])
with col1:
    st.dataframe(df.head(10)[cols_display])

with col2:
    for _, row in df.head(3).iterrows():
        st.markdown(f"""
        #### 📍 Station {row['Station']}
        - 🌡️ Température : {row['AIR TEMPERATURE']}°C {get_weather_icon(float(row['AIR TEMPERATURE']))}
        - 💧 Humidité : {row['HUMIDITY']}% {"🔴" if float(row['HUMIDITY']) > 98 else "💧"}
        - 💨 Vent : {row['WIND SPEED']} m/s
        - 🧭 Pression : {row['AIR PRESSURE']} hPa
        """)
        if "TIDE HEIGHT" in row:
            st.markdown(f"- 🌊 Marée : {row['TIDE HEIGHT']} m")
        if "SURGE" in row:
            st.markdown(f"- ⚠️ SURGE : {row['SURGE']} m")

# === 🗺️ Carte interactive ===
st.subheader("🗺️ Carte interactive des stations météo")
m = folium.Map(location=[4.05, 9.68], zoom_start=10)
for _, row in df.groupby("Station").first().reset_index().iterrows():
    popup = f"""
    <b>{row['Station']}</b><br>
    Temp: {row['AIR TEMPERATURE']} °C<br>
    Vent: {row['WIND SPEED']} m/s<br>
    Humidité: {row['HUMIDITY']} %<br>
    """
    if "TIDE HEIGHT" in row:
        popup += f"Marée: {row['TIDE HEIGHT']} m<br>"
    if "SURGE" in row:
        popup += f"SURGE: {row['SURGE']} m"
    folium.Marker(
        location=[row["Latitude"], row["Longitude"]],
        popup=popup,
        tooltip=row["Station"],
        icon=folium.Icon(color="blue", icon="cloud")
    ).add_to(m)
st_folium(m, width=900, height=500)

# === 📈 Graphique évolutif par station ===
st.subheader("📈 Graphique par station et paramètre")
station_selected = st.selectbox("Station", df["Station"].unique())
params = ["AIR TEMPERATURE", "HUMIDITY", "WIND SPEED", "AIR PRESSURE"]
if "TIDE HEIGHT" in df.columns: params.append("TIDE HEIGHT")
if "SURGE" in df.columns: params.append("SURGE")

param = st.selectbox("Paramètre", params)
fig = px.line(df[df["Station"] == station_selected], x="DateTime", y=param, title=f"{param} à {station_selected}")
st.plotly_chart(fig, use_container_width=True)

# === 📊 Comparaison entre stations ===
st.subheader("📊 Comparaison multistation")
for p in params:
    fig = px.line(df, x="DateTime", y=p, color="Station", title=f"Comparaison – {p}")
    st.plotly_chart(fig, use_container_width=True)

# === 🌐 Mini-carte météo Windy ===
st.subheader("🌐 Carte météo animée – Windy")
st.components.v1.html('''
<iframe width="100%" height="450" src="https://embed.windy.com/embed2.html?lat=4.05&lon=9.68&detailLat=4.05&detailLon=9.68&zoom=9&type=wind" frameborder="0"></iframe>
''', height=450)

# === 💾 Téléchargement des données ===
st.subheader("💾 Télécharger les données météo")
csv = df.to_csv(index=False).encode("utf-8")
st.download_button("📥 Télécharger au format CSV", data=csv, file_name="meteo_douala.csv", mime="text/csv")
