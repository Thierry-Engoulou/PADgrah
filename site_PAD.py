# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sqlite3
import numpy as np
from datetime import datetime, timedelta
import uuid
import os
import requests
import smtplib
import io
import zipfile
import time
import concurrent.futures
import pyarrow
import xarray as xr
import tempfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ==========================================================
# CONFIG
# ==========================================================

st.set_page_config(
    page_title="Météo Douala",
    layout="wide"
)

PARQUET_CACHE = "valide.parquet"
API_URL = "https://data-real-time-6.onrender.com/donnees"
BATCH_SIZE = 10000

# Session HTTP globale pour le pooling de connexions
http_session = requests.Session()


# ==========================================================
# SQLITE
# ==========================================================

conn = sqlite3.connect("demandes.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
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
""")
conn.commit()


# ==========================================================
# EMAIL
# ==========================================================

def envoyer_email(dest, sujet, contenu):
    if isinstance(dest, str):
        dest_list = [dest]
    else:
        dest_list = dest

    expediteur = "engoulouthierry62@gmail.com"
    mot_de_passe = "tfzybsaqrlyntkox"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = sujet
    msg["From"] = expediteur
    msg["To"] = ", ".join(dest_list)
    msg.attach(MIMEText(contenu, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(expediteur, mot_de_passe)
            for d in dest_list:
                s.sendmail(expediteur, d, msg.as_string())
    except Exception as e:
        st.error(f"Erreur Email : {e}")


# ==========================================================
# FILTRES ET MODELES SCIENTIFIQUES
# ==========================================================

def normaliser_colonnes(df):
    """Standardise les noms de colonnes pour éviter les KeyError."""
    if df.empty:
        return df
    mapping = {
        "STATION NAME": "Station",
        "TIDE_HEIGHT": "TIDE HEIGHT",
        "WIND_SPEED": "WIND SPEED",
        "WIND_DIR": "WIND DIR",
        "AIR_PRESSURE": "AIR PRESSURE",
        "AIR_TEMPERATURE": "AIR TEMPERATURE",
        "HUMIDITY_RELATIVE": "HUMIDITY"
    }
    df = df.rename(columns=mapping)
    return df


def appliquer_filtres_scientifiques(df):
    """Supprime les valeurs aberrantes (outliers) via la méthode IQR."""
    df = df.copy()
    params = ["TIDE HEIGHT", "WIND SPEED", "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]
    for p in params:
        if p in df.columns:
            temp = df[p].dropna()
            if not temp.empty:
                Q1 = temp.quantile(0.25)
                Q3 = temp.quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 2.5 * IQR
                upper = Q3 + 2.5 * IQR
                df.loc[(df[p] < lower) | (df[p] > upper), p] = np.nan
            df[p] = df[p].interpolate(method="linear")
    return df


def calculer_modele_harmonique(df, station):
    """Génère une courbe sinusoïdale pure basée sur les données réelles."""
    df_st = df[df["Station"] == station].dropna(subset=["TIDE HEIGHT"])
    if len(df_st) < 10:
        return None
    t0 = df_st["DateTime"].min()
    t = (df_st["DateTime"] - t0).dt.total_seconds() / 3600.0
    h = df_st["TIDE HEIGHT"].values
    omega = 2 * np.pi / 12.4206
    X = np.column_stack([np.ones(len(t)), np.cos(omega * t), np.sin(omega * t)])
    coeffs, _, _, _ = np.linalg.lstsq(X, h, rcond=None)
    h0, A, B = coeffs
    t_full = (df["DateTime"] - t0).dt.total_seconds() / 3600.0
    return h0 + A * np.cos(omega * t_full) + B * np.sin(omega * t_full)


# ==========================================================
# TELECHARGEMENT API VITE ECLAIR (PARALLELE)
# ==========================================================

def fetch_all_data(start=None, end=None):
    d_start = pd.to_datetime(start) if start else pd.to_datetime("2024-01-01")
    d_end = pd.to_datetime(end) if end else datetime.today()

    # Découpage intelligent par semaines
    weeks = pd.date_range(start=d_start, end=d_end, freq="7D").tolist()
    if not weeks or weeks[-1] < d_end:
        weeks.append(d_end)

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    total_steps = max(len(weeks) - 1, 1)

    def fetch_week(i):
        w_start = weeks[i].strftime("%Y-%m-%d")
        w_end = weeks[i + 1].strftime("%Y-%m-%d")
        params = {"limit": BATCH_SIZE, "start": w_start, "end": w_end}
        
        for attempt in range(5):
            try:
                # Augmentation du timeout à 60s pour Render
                r = http_session.get(API_URL, params=params, timeout=60)
                if r.status_code == 200:
                    resp = r.json()
                    # Si l'API renvoie un message d'erreur de filtrage
                    if isinstance(resp, dict) and "message" in resp:
                        st.sidebar.warning(f"⚠️ {w_start} : {resp['message']}")
                    
                    if isinstance(resp, list):
                        return resp
                    return resp.get("data", []) if isinstance(resp, dict) else []
                else:
                    st.sidebar.error(f"Erreur API {r.status_code} sur {w_start}")
                time.sleep(1)
            except Exception as e:
                st.sidebar.error(f"Échec Connexion {w_start} : {e}")
                time.sleep(2)
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_week, i): i for i in range(total_steps)}
        for count, future in enumerate(concurrent.futures.as_completed(futures)):
            res = future.result()
            if res:
                all_data.extend(res)
            progress_bar.progress(min((count + 1) / total_steps, 1.0))
            status_text.text(f"⚡ Vitesse Éclair : {count + 1}/{total_steps} blocs ({len(all_data)} lignes)")

    progress_bar.empty()
    status_text.empty()
    return all_data


# ==========================================================
# SYNC CACHE
# ==========================================================

def sync_cache(start=None, end=None):
    st.sidebar.write(f"🔍 Sync: {start} ➔ {end}")
    data = fetch_all_data(start, end)
    if not data:
        st.info(f"Aucune donnée trouvée sur le Cloud pour la période : {start} au {end}")
        return
    
    df_new = pd.DataFrame(data)
    df_new = normaliser_colonnes(df_new)
    
    if not df_new.empty and "DateTime" in df_new.columns:
        df_new["DateTime"] = pd.to_datetime(df_new["DateTime"])

    if os.path.exists(PARQUET_CACHE):
        try:
            df_old = pd.read_parquet(PARQUET_CACHE)
            df_old["DateTime"] = pd.to_datetime(df_old["DateTime"])
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            df_combined = df_new
    else:
        df_combined = df_new

    if not df_combined.empty and "DateTime" in df_combined.columns:
        df_combined.drop_duplicates(subset=["DateTime", "Station"], inplace=True)
        df_combined.sort_values("DateTime", inplace=True)
        # Écriture Parquet ultra-rapide
        df_combined.to_parquet(PARQUET_CACHE, index=False, engine="pyarrow")
    
    st.cache_data.clear()


# ==========================================================
# CHARGEMENT DES DONNEES
# ==========================================================

def load_data(start=None, end=None):
    # Si le cache Parquet n'existe pas, on synchronise la période demandée
    if not os.path.exists(PARQUET_CACHE) or os.path.getsize(PARQUET_CACHE) == 0:
        sync_cache(start, end)

    try:
        df = pd.read_parquet(PARQUET_CACHE)
    except Exception:
        st.warning("Initialisation de la base de données locale...")
        sync_cache(start, end)
        try:
            df = pd.read_parquet(PARQUET_CACHE)
        except Exception:
            return pd.DataFrame()

    df = normaliser_colonnes(df)
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    
    # === Diagnostic Scientifique (User Tip) ===
    if not df.empty:
        st.sidebar.info(f"📅 Base locale : {df['DateTime'].min().strftime('%Y-%m-%d')} au {df['DateTime'].max().strftime('%Y-%m-%d')}")
    
    df = appliquer_filtres_scientifiques(df)

    if start and end:
        s_dt = pd.to_datetime(start)
        e_dt = pd.to_datetime(end)
        mask = (df["DateTime"] >= s_dt) & (df["DateTime"] <= e_dt)
        
        # Si on n'a presque pas de données pour la plage demandée, on force une sync
        if len(df[mask]) < 5:
            sync_cache(start, end)
            try:
                df = pd.read_parquet(PARQUET_CACHE)
                df = normaliser_colonnes(df)
                df["DateTime"] = pd.to_datetime(df["DateTime"])
                mask = (df["DateTime"] >= s_dt) & (df["DateTime"] <= e_dt)
            except Exception:
                pass
        
        # === Filtrage Robuste (User Tip) ===
        if len(df[mask]) == 0:
            st.warning(f"⚠️ Aucune donnée entre {start} et {end}. Affichage des dernières données disponibles.")
        else:
            df = df[mask]

    # Conversion numérique scientifique optimisée
    cols = ["TIDE HEIGHT", "WIND SPEED", "WIND DIR", "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df.sort_values("DateTime", inplace=True)
    return df


# ==========================================================
# DOWNSAMPLING
# ==========================================================

def downsample(df, max_points=2000):
    if len(df) <= max_points:
        return df
    step = len(df) // max_points
    return df.iloc[::step]


# ==========================================================
# GRAPHES
# ==========================================================

params_list = ["TIDE HEIGHT", "WIND SPEED", "WIND DIR", "AIR PRESSURE", "AIR TEMPERATURE", "DEWPOINT", "HUMIDITY"]

STATION_COLORS = {
    "SM 2": "blue",
    "SM 3": "crimson",
    "SM 4": "green",
}


def afficher_graphes(df):
    if df.empty:
        st.warning("Aucune donnée")
        return

    window = st.sidebar.slider("Lissage", 1, 51, 5)
    no_downsample = st.sidebar.checkbox("Désactiver échantillonnage")

    for p in params_list:
        if p not in df.columns:
            continue

        fig = go.Figure()

        for station in df["Station"].unique():
            d = df[df["Station"] == station].copy()
            if not no_downsample:
                d = downsample(d, 2000)

            color = STATION_COLORS.get(station, "orange")

            if p == "TIDE HEIGHT":
                h_harmonic = calculer_modele_harmonique(d, station)
                if h_harmonic is not None:
                    fig.add_trace(go.Scattergl(
                        x=d["DateTime"], y=h_harmonic,
                        name=f"{station} (Modèle)",
                        line=dict(width=3, color=color),
                        mode="lines"
                    ))
                fig.add_trace(go.Scattergl(
                    x=d["DateTime"], y=d[p],
                    name=f"{station} (Données)",
                    mode="markers",
                    marker=dict(size=4, opacity=0.4, color=color)
                ))
            else:
                d["display"] = d[p].rolling(window, center=True, min_periods=1).mean()
                fig.add_trace(go.Scattergl(
                    x=d["DateTime"], y=d["display"],
                    name=station,
                    line=dict(color=color),
                    mode="lines" if not no_downsample else "lines+markers"
                ))

        fig.update_layout(
            title=p,
            hovermode="x unified",
            template="plotly_dark" if p == "TIDE HEIGHT" else "plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# SIDEBAR - MAINTENANCE
# ==========================================================
with st.sidebar:
    st.divider()
    st.subheader("🛠 Maintenance")
    if st.button("🚨 SYNCHRONISATION TOTALE (SANS FILTRE)"):
        with st.spinner("Récupération de TOUTE la base (peut être lent)..."):
            # On appelle fetch_all_data sans start/end
            data = fetch_all_data(None, None)
            if data:
                df_new = pd.DataFrame(data)
                df_new = normaliser_colonnes(df_new)
                df_new["DateTime"] = pd.to_datetime(df_new["DateTime"])
                df_new.to_parquet(PARQUET_CACHE, index=False)
                st.success(f"Base reconstruite : {len(df_new)} lignes.")
                st.rerun()
            else:
                st.error("Échec de la récupération totale.")

st.title("Dashboard météo PAD")

tab1, tab2, tab3, tab4 = st.tabs([
    "7 jours",
    "Période personnalisée",
    "Télécharger",
    "Administration 🔐"
])


# ==========================================================
# TAB 1 (7 JOURS)
# ==========================================================

with tab1:
    end_7 = datetime.today()
    start_7 = end_7 - timedelta(days=7)
    
    # ÉTAT DE SYNCHRONISATION
    sync_key_7 = f"synced_7d_{start_7.strftime('%Y%m%d')}"
    if sync_key_7 not in st.session_state:
        st.session_state[sync_key_7] = False

    # ÉTAPE 1 : SYNCHRONISATION
    st.markdown("### ☁️ Étape 1 : Récupération")
    if st.button("📥 Synchroniser les 7 derniers jours depuis le Cloud"):
        with st.spinner("Récupération en cours..."):
            sync_cache(start_7, end_7)
            st.session_state[sync_key_7] = True
            # On récupère le nombre de lignes pour l'info
            df_temp = load_data(start_7, end_7)
            st.success(f"✅ Synchronisation terminée : {len(df_temp)} lignes récupérées.")
            st.rerun()

    # ÉTAPE 2 : VISUALISATION (Conditionnelle)
    if st.session_state[sync_key_7]:
        st.divider()
        st.markdown("### 📊 Étape 2 : Visualisation")
        if st.button("📈 Afficher les Graphiques (7j)"):
            df = load_data(start_7, end_7)
            if not df.empty:
                with st.expander("🔍 Aperçu technique des données"):
                    st.dataframe(df.head(10))
                afficher_graphes(df)
            else:
                st.warning("Aucune donnée à afficher pour cette période.")


# ==========================================================
# TAB 2 (PERSONNALISE)
# ==========================================================

with tab2:
    col1, col2 = st.columns(2)
    p_start = col1.date_input("Début", datetime.today() - timedelta(days=30), key="p_start")
    p_end = col2.date_input("Fin", datetime.today(), key="p_end")

    # ÉTAT DE SYNCHRONISATION
    sync_key_p = f"synced_custom_{p_start}_{p_end}"
    if sync_key_p not in st.session_state:
        st.session_state[sync_key_p] = False

    # ÉTAPE 1 : SYNCHRONISATION
    st.markdown("### ☁️ Étape 1 : Récupération")
    if st.button("📥 Synchroniser la période depuis le Cloud"):
        with st.spinner("Récupération en cours..."):
            sync_cache(p_start, p_end)
            st.session_state[sync_key_p] = True
            df_temp = load_data(p_start, p_end)
            st.success(f"✅ Synchronisation terminée : {len(df_temp)} lignes récupérées.")
            st.rerun()

    # ÉTAPE 2 : VISUALISATION
    if st.session_state[sync_key_p]:
        st.divider()
        st.markdown("### 📊 Étape 2 : Visualisation")
        if st.button("📈 Afficher les Graphiques (Période)"):
            df = load_data(p_start, p_end)
            if not df.empty:
                with st.expander("🔍 Aperçu technique des données"):
                    st.dataframe(df.head(10))
                afficher_graphes(df)
            else:
                st.warning("Aucune donnée à afficher pour cette période.")


# ==========================================================
# TAB 3 — TELECHARGEMENT AVEC VALIDATION ADMIN
# ==========================================================

with tab3:
    st.subheader("Demande d'accès aux données")

    if "req_id" not in st.session_state:
        st.session_state.req_id = None

    if not st.session_state.req_id:
        with st.form("request_form"):
            st.info("Formulaire de demande d'accès — Validation administrateur requise")
            nom = st.text_input("Nom / Institution")
            email_user = st.text_input("Votre Email")
            raison = st.text_area("Motif de l'utilisation")
            submit = st.form_submit_button("Envoyer la demande")

            if submit and nom and email_user and raison:
                req_id = str(uuid.uuid4())[:8]
                cursor.execute("INSERT INTO demandes VALUES (?,?,?,?,?,?,?,?)",
                               (req_id, nom, "", email_user, raison, "en_attente", "", time.time()))
                conn.commit()
                envoyer_email(
                    "engoulouthierry62@gmail.com",
                    f"DEMANDE PAD - {nom} [{req_id}]",
                    f"<b>ID:</b> {req_id}<br><b>Nom:</b> {nom}<br><b>Email:</b> {email_user}<br><b>Motif:</b> {raison}"
                )
                st.session_state.req_id = req_id
                st.success("Demande envoyée ! L'administrateur examinera votre demande et vous recevrez un email.")
                st.rerun()

    else:
        cursor.execute("SELECT statut FROM demandes WHERE id=?", (st.session_state.req_id,))
        row = cursor.fetchone()
        statut = row[0] if row else "inconnu"

        if statut == "en_attente":
            st.warning(f"🕒 Votre demande ({st.session_state.req_id}) est en cours de traitement.")
            if st.button("Actualiser le statut"):
                st.rerun()
        elif statut == "refuse":
            st.error("❌ Votre demande a été refusée par l'administrateur.")
            if st.button("Faire une nouvelle demande"):
                st.session_state.req_id = None
                st.rerun()
        elif statut == "valide":
            st.success("✅ Accès débloqué par l'administrateur !")
            st.divider()
            
            format_choisi = st.radio("Format d'exportation", ["ZIP (CSV)", "NetCDF (.nc)"], horizontal=True)

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### Par période")
                s_dl = st.date_input("Début export", datetime.today() - timedelta(days=30), key="dl_start")
                e_dl = st.date_input("Fin export", datetime.today(), key="dl_end")

                if st.button("Préparer fichier (Période)"):
                    sync_cache(s_dl, e_dl)
                    df = load_data(s_dl, e_dl)
                    if not df.empty:
                        if format_choisi == "ZIP (CSV)":
                            csv_buffer = io.StringIO()
                            df.to_csv(csv_buffer, index=False)
                            zip_buffer = io.BytesIO()
                            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zf:
                                zf.writestr("meteo_export.csv", csv_buffer.getvalue())
                            st.download_button("Télécharger ZIP", zip_buffer.getvalue(), "meteo.zip", "application/zip")
                        else:
                            # Export NetCDF
                            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                                ds = df.set_index(['DateTime', 'Station']).to_xarray()
                                ds.attrs['title'] = 'Données Météo PAD'
                                ds.to_netcdf(tmp.name, engine="h5netcdf")
                                tmp_path = tmp.name
                            with open(tmp_path, "rb") as f:
                                st.download_button("Télécharger NetCDF (.nc)", f.read(), "meteo.nc", "application/x-netcdf")
                            os.remove(tmp_path)
                    else:
                        st.warning("Aucune donnée")

            with col2:
                st.markdown("### Toute la base")
                if st.button("Préparer TOUTE LA BASE"):
                    sync_cache()
                    df = load_data()
                    if not df.empty:
                        if format_choisi == "ZIP (CSV)":
                            csv_buffer = io.StringIO()
                            df.to_csv(csv_buffer, index=False)
                            zip_buffer = io.BytesIO()
                            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zf:
                                zf.writestr("meteo_full.csv", csv_buffer.getvalue())
                            st.download_button("Télécharger TOUT (ZIP)", zip_buffer.getvalue(), "full_history.zip", "application/zip")
                        else:
                            # Export NetCDF
                            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                                ds = df.set_index(['DateTime', 'Station']).to_xarray()
                                ds.to_netcdf(tmp.name, engine="h5netcdf")
                                tmp_path = tmp.name
                            with open(tmp_path, "rb") as f:
                                st.download_button("Télécharger TOUT (NetCDF)", f.read(), "full_history.nc", "application/x-netcdf")
                            os.remove(tmp_path)


# ==========================================================
# TAB 4 — ADMINISTRATION
# ==========================================================

with tab4:
    st.subheader("Gestion des demandes d'accès")
    password = st.text_input("Mot de passe Administrateur", type="password", key="admin_pwd")

    if password == "ADMIN_PAD_2024":
        cursor.execute("SELECT * FROM demandes WHERE statut='en_attente' ORDER BY timestamp DESC")
        demandes = cursor.fetchall()

        if not demandes:
            st.info("Aucune demande en attente.")
        else:
            for d in demandes:
                with st.expander(f"📋 {d[1]} — {d[3]}"):
                    st.write(f"**ID :** {d[0]}")
                    st.write(f"**Motif :** {d[4]}")
                    c1, c2 = st.columns(2)
                    if c1.button("✅ Valider", key=f"val_{d[0]}"):
                        cursor.execute("UPDATE demandes SET statut='valide' WHERE id=?", (d[0],))
                        conn.commit()
                        envoyer_email(
                            d[3],
                            "✅ Demande de données PAD Approuvée",
                            f"Bonjour {d[1]},<br><br>Votre demande d'accès aux données PAD a été <b>approuvée</b>.<br>Retournez sur le dashboard pour télécharger vos données.<br><br>Cordialement,<br>PAD Douala"
                        )
                        st.success(f"Demande {d[0]} validée et email envoyé.")
                        st.rerun()
                    if c2.button("❌ Refuser", key=f"ref_{d[0]}"):
                        cursor.execute("UPDATE demandes SET statut='refuse' WHERE id=?", (d[0],))
                        conn.commit()
                        envoyer_email(
                            d[3],
                            "❌ Demande de données PAD Refusée",
                            f"Bonjour {d[1]},<br><br>Nous regrettons de vous informer que votre demande d'accès aux données a été <b>refusée</b>.<br><br>Cordialement,<br>PAD Douala"
                        )
                        st.error(f"Demande {d[0]} refusée et email envoyé.")
                        st.rerun()
    elif password:
        st.error("Mot de passe incorrect")


# ==========================================================
# CARTE
# ==========================================================

st.subheader("Carte météo")

st.components.v1.html(
"""
<div id="map" style="height:500px;"></div>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
var map = L.map('map').setView([3.848, 11.502], 12);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'© OSM'}).addTo(map);
var stations=[
    {name:"SM 2",lat:3.8480,lng:11.5021},
    {name:"SM 3",lat:3.7601,lng:11.3803},
    {name:"SM 4",lat:3.9833,lng:11.3166}
];
stations.forEach(function(s){
    L.marker([s.lat,s.lng]).addTo(map).bindPopup("<b>"+s.name+"</b>").openPopup();
});
</script>
""",
height=520
)
