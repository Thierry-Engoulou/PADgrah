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
import threading
import base64

# URL exacte du Dashboard Streamlit (Mettre à jour si vous déployez l'application en ligne)
# L'API étant sur data-real-time-2, le Dashboard a sûrement une URL différente (ex: meteo-pad.onrender.com).
APP_URL = "http://localhost:8501"

# ==========================================================
# CONFIG
# ==========================================================

st.set_page_config(
    page_title="Météo Douala",
    layout="wide"
)

PARQUET_CACHE = "valide.parquet"
API_URL = "https://data-real-time-2.onrender.com/donnees"
BATCH_SIZE = 6000

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

    import socket
    orig_getaddrinfo = socket.getaddrinfo
    
    # Forcer IPv4 (contourne l'erreur [Errno 101] Network is unreachable)
    def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        return orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = getaddrinfo_ipv4

    success = False
    try:
        # Tentative 1 : SSL sur port 465
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as s:
            s.login(expediteur, mot_de_passe)
            s.sendmail(expediteur, dest_list, msg.as_string())
        success = True
    except Exception as e_ssl:
        try:
            # Tentative 2 : TLS sur port 587 (Secours)
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as s:
                s.starttls()
                s.login(expediteur, mot_de_passe)
                s.sendmail(expediteur, dest_list, msg.as_string())
            success = True
        except Exception as e_tls:
            st.error(f"Erreur d'envoi d'email : SSL({e_ssl}) | TLS({e_tls})")
            success = False
    finally:
        # Restaurer la configuration réseau d'origine
        socket.getaddrinfo = orig_getaddrinfo

    return success

# ==========================================================
# EMAILS PREMIUM & TEMPLATES
# ==========================================================

def generer_html_email(type_msg, nom, req_id, details=None):
    """Génère un template HTML professionnel Premium avec dégradés et boutons."""
    # Couleurs institutionnelles PAD
    color_primary = "#004B8D"  # Bleu Royal
    color_secondary = "#D4AF37" # Or
    color_accent = "#1C2D4A"    # Marine
    
    if type_msg == "admin_nouvelle_demande":
        sujet = f"🚨 Nouvelle Demande d'Accès PAD - {nom}"
        libelle_status = "NOUVELLE DEMANDE"
        msg_body = f"""
            <p style="font-size: 16px; color: #555;">Une nouvelle demande de téléchargement a été déposée par <strong>{nom}</strong>.</p>
            <div style="background-color: #f9f9f9; border-left: 4px solid {color_primary}; padding: 15px; margin: 20px 0;">
                <strong>ID Demande :</strong> {req_id}<br>
                <strong>Motif :</strong> {details}<br>
            </div>
            <p style="font-size: 14px; color: #777;">Veuillez valider ou refuser cette demande depuis votre tableau de bord ou via les boutons ci-dessous :</p>
            <div style="text-align: center; margin-top: 30px;">
                <a href="{APP_URL}/?action=valider&req_id={req_id}" style="background-color: #28a745; color: white; padding: 12px 25px; text-decoration: none; border-radius: 6px; font-weight: bold; margin-right: 10px;">✅ ACCEPTER</a>
                <a href="{APP_URL}/?action=refuser&req_id={req_id}" style="background-color: #dc3545; color: white; padding: 12px 25px; text-decoration: none; border-radius: 6px; font-weight: bold;">❌ REFUSER</a>
            </div>
        """
    elif type_msg == "user_approuve":
        sujet = "✅ Votre accès aux données PAD est PRÊT"
        libelle_status = "DEMANDE APPROUVÉE"
        msg_body = f"""
            <p style="font-size: 16px; color: #555;">Bonjour <strong>{nom}</strong>,</p>
            <p style="font-size: 16px; color: #555;">Nous avons le plaisir de vous informer que l'Administration du PAD a <strong>validé</strong> votre demande d'accès.</p>
            <p style="font-size: 16px; color: #555; margin-bottom: 30px;">Vous pouvez maintenant télécharger l'historique complet en cliquant sur le bouton ci-dessous :</p>
            <div style="text-align: center; margin: 40px 0;">
                <a href="{APP_URL}/?dl_req_id={req_id}&format=excel" style="background: linear-gradient(135deg, #28a745, #218838); color: white; padding: 18px 35px; text-decoration: none; border-radius: 50px; font-weight: bold; font-size: 18px; box-shadow: 0 4px 15px rgba(40, 167, 69, 0.3);">📥 TÉLÉCHARGER LE FICHIER EXCEL</a>
            </div>
            <p style="text-align: center; font-size: 14px; color: #999;">Ou <a href="{APP_URL}/?dl_req_id={req_id}" style="color: {color_primary};">cliquez ici</a> pour configurer une période spécifique sur le site.</p>
        """
    else:  # refuse
        sujet = "❌ Statut de votre demande d'accès PAD"
        libelle_status = "DEMANDE NON-RETENUE"
        msg_body = f"""
            <p style="font-size: 16px; color: #555;">Bonjour <strong>{nom}</strong>,</p>
            <p style="font-size: 16px; color: #555;">Nous vous remercions pour votre intérêt. Cependant, l'Administration ne peut pas donner suite à votre demande pour le moment.</p>
            <p style="font-size: 14px; color: #777; margin-top: 20px;">Pour toute question supplémentaire, n'hésitez pas à nous contacter directement.</p>
        """

    html = f"""
    <html>
    <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7f6; margin: 0; padding: 30px;">
        <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,0.1);">
            <div style="background-color: {color_primary}; padding: 30px; text-align: center; color: white;">
                <h2 style="margin: 0; letter-spacing: 2px; font-weight: 300;">PORT AUTONOME DE DOUALA</h2>
                <div style="display: inline-block; margin-top: 10px; padding: 5px 15px; background: {color_secondary}; color: {color_accent}; font-size: 11px; font-weight: bold; border-radius: 20px;">{libelle_status}</div>
            </div>
            <div style="padding: 40px;">
                {msg_body}
            </div>
            <div style="background-color: #fcfcfc; padding: 20px; text-align: center; border-top: 1px solid #eee; color: #aaa; font-size: 12px;">
                Ceci est une notification automatique. Veuillez ne pas répondre directement.<br>
                © 2024 Port Autonome de Douala - Direction de la Météorologie
            </div>
        </div>
    </body>
    </html>
    """
    return sujet, html


# ==========================================================
# ROUTAGE PAR LIENS EMAILS (VALIDATION / REFUS ADMIN)
# ==========================================================
params = st.query_params
if "action" in params and "req_id" in params:
    action = params["action"]
    req_id = params["req_id"]
    cursor.execute("SELECT nom, email, statut FROM demandes WHERE id=?", (req_id,))
    row = cursor.fetchone()
    if row:
        c_nom, c_email, c_statut = row
        if c_statut == "en_attente":
            if action == "valider":
                cursor.execute("UPDATE demandes SET statut='valide' WHERE id=?", (req_id,))
                conn.commit()
                sujet, msg_user = generer_html_email("user_approuve", c_nom, req_id)
                envoyer_email(c_email, sujet, msg_user)
                st.success(f"✅ Demande #{req_id} VALIDÉE ! Un email Premium a été envoyé au demandeur.")
            elif action == "refuser":
                cursor.execute("UPDATE demandes SET statut='refuse' WHERE id=?", (req_id,))
                conn.commit()
                sujet, msg_user = generer_html_email("user_refuse", c_nom, req_id)
                envoyer_email(c_email, sujet, msg_user)
                st.error(f"❌ Demande #{req_id} REFUSÉE ! Un email de notification a été envoyé.")
            
            if st.button("⬅️ Retour au Dashboard"):
                st.query_params.clear()
                st.rerun()
            st.stop()
        else:
            st.info(f"ℹ️ La demande #{req_id} a déjà été traitée (Statut: {c_statut}).")
            if st.button("⬅️ Retour au Dashboard"):
                st.query_params.clear()
                st.rerun()
            st.stop()
    else:
        st.warning(f"⚠️ Demande #{req_id} introuvable.")
        if st.button("⬅️ Retour au Dashboard"):
            st.query_params.clear()
            st.rerun()
        st.stop()

if "dl_req_id" in params:
    st.session_state.req_id = params["dl_req_id"]
    if "format" in params:
        # Pour une réactivité immédiate sans recharger
        st.session_state.dl_format = params["format"]


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

    # Découpage intelligent par jours pour éviter les Timeouts (Render + MongoDB)
    days = pd.date_range(start=d_start, end=d_end, freq="1D").tolist()
    if not days or days[-1] < d_end:
        days.append(d_end)

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    total_steps = max(len(days) - 1, 1)

    def fetch_chunk(i):
        c_start = days[i].strftime("%Y-%m-%d")
        c_end = days[i + 1].strftime("%Y-%m-%d")
        params = {"limit": BATCH_SIZE, "start": c_start, "end": c_end}
        
        for attempt in range(4):
            try:
                # Augmentation du timeout à 60s pour Render
                r = http_session.get(API_URL, params=params, timeout=60)
                if r.status_code == 200:
                    resp = r.json()
                    if isinstance(resp, dict) and "message" in resp:
                        # Log plus discret car c'est normal d'avoir des jours vides
                        pass
                    
                    if isinstance(resp, list):
                        return resp
                    return resp.get("data", []) if isinstance(resp, dict) else []
                elif r.status_code == 429:
                    st.sidebar.warning(f"🚦 Rate Limit sur {c_start}. Pause...")
                    time.sleep(3 + attempt * 2)
                else:
                    st.sidebar.error(f"Erreur API {r.status_code} sur {c_start}")
                time.sleep(0.5)
            except Exception as e:
                st.sidebar.error(f"Échec Connexion {c_start} : {e}")
                time.sleep(1)
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_chunk, i): i for i in range(total_steps)}
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

@st.cache_data(show_spinner=False)
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
        
        # === Filtrage Robuste (User Tip) ===
        if len(df[mask]) == 0:
            st.warning(f"⚠️ Aucune donnée entre {start} et {end} dans le cache local. Veuillez cliquer sur 'Synchroniser' (Étape 1) si besoin.")
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


st.title("Admin & Téléchargement PAD")

tab1, tab2 = st.tabs([
    "Télécharger",
    "Administration 🔐"
])


with tab1:
    st.subheader("Accès aux données sécurisé")
    st.info("⚠️ Pour télécharger les données de **plus de 7 jours**, veuillez faire une demande à l'Administration du PAD en remplissant ce formulaire.")

    if "req_id" not in st.session_state:
        st.session_state.req_id = None

    if not st.session_state.req_id:
        with st.form("request_form"):
            nom = st.text_input("Nom / Institution")
            email_user = st.text_input("Votre Email")
            raison = st.text_area("Motif de l'utilisation")
            submit = st.form_submit_button("Envoyer la demande d'accès")

            if submit and nom and email_user and raison:
                req_id = str(uuid.uuid4())[:8]
                cursor.execute("INSERT INTO demandes VALUES (?,?,?,?,?,?,?,?)",
                               (req_id, nom, "", email_user, raison, "en_attente", "", time.time()))
                conn.commit()
                
                lien_val = f"{APP_URL}/?action=valider&req_id={req_id}"
                lien_ref = f"{APP_URL}/?action=refuser&req_id={req_id}"
                
                sujet, msg_admin = generer_html_email("admin_nouvelle_demande", nom, req_id, raison)
                success = envoyer_email(
                    ["engoulouthierry62@gmail.com", "ulrichlangoul7@gmail.com"],
                    sujet,
                    msg_admin
                )
                
                if success:
                    st.session_state.req_id = req_id
                    st.rerun()
                else:
                    st.error("⚠️ L'envoi de l'email à l'administrateur a échoué. Veuillez vérifier la connexion ou l'adresse email.")
    else:
        cursor.execute("SELECT statut FROM demandes WHERE id=?", (st.session_state.req_id,))
        row = cursor.fetchone()
        statut = row[0] if row else "inconnu"

        if statut == "en_attente":
            st.warning(f"🕒 Votre demande (#{st.session_state.req_id}) est en cours d'examen par l'administrateur.")
            if st.button("🔄 Actualiser le statut"):
                st.rerun()
        elif statut == "refuse":
            st.error("❌ Votre demande a été refusée.")
            if st.button("📝 Faire une nouvelle demande"):
                st.session_state.req_id = None
                st.rerun()
        elif statut == "valide":
            # === EXPÉRIENCE PREMIUM : TÉLÉCHARGEMENT DIRECT ===
            if "dl_format" in st.session_state and st.session_state.dl_format:
                fmt = st.session_state.dl_format
                st.session_state.dl_format = None
                
                st.toast(f"🚀 Préparation de votre fichier {fmt.upper()}...", icon="🪄")
                with st.status(f"✨ Génération de votre fichier {fmt.upper()} en cours...", expanded=True) as status:
                    st.write("🔍 Analyse de la base historique...")
                    df = load_data()
                    st.write("🧊 Formatage des données scientifiques...")
                    if not df.empty:
                        if fmt == "excel":
                            xlsx_buffer = io.BytesIO()
                            with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
                                df.to_excel(writer, index=False, sheet_name="Meteo_Full")
                            b64 = base64.b64encode(xlsx_buffer.getvalue()).decode()
                            href = f'<a id="auto_dl" href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="full_history_pad.xlsx"><button style="padding:15px; font-size:18px; background-color:#28a745; color:white; border:none; border-radius:8px; cursor:pointer;">📥 Cliquez ici pour télécharger EXCEL</button></a>'
                            st.markdown(href, unsafe_allow_html=True)
                            st.components.v1.html("<script>var a = window.parent.document.getElementById('auto_dl'); if(a) a.click();</script>", height=0)
                        elif fmt == "netcdf":
                            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                                ds = df.set_index(['DateTime', 'Station']).to_xarray()
                                ds.to_netcdf(tmp.name, engine="h5netcdf")
                                tmp_path = tmp.name
                            with open(tmp_path, "rb") as f:
                                b64 = base64.b64encode(f.read()).decode()
                            os.remove(tmp_path)
                            href = f'<a id="auto_dl" href="data:application/x-netcdf;base64,{b64}" download="full_history.nc"><button style="padding:15px; font-size:18px; background-color:#17a2b8; color:white; border:none; border-radius:8px; cursor:pointer;">📥 Cliquez ici pour télécharger NETCDF</button></a>'
                            st.markdown(href, unsafe_allow_html=True)
                            st.components.v1.html("<script>var a = window.parent.document.getElementById('auto_dl'); if(a) a.click();</script>", height=0)
                    status.update(label="✅ Fichier Prêt ! Le téléchargement a démarré.", state="complete", expanded=False)
                st.balloons()
            
            st.success("✅ Accès autorisé ! Vous pouvez maintenant télécharger les données.")
                            
            st.divider()
            format_choisi = st.radio("Format d'exportation", ["Excel (.xlsx)", "NetCDF (.nc)"], horizontal=True)

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### 📅 Par période")
                s_dl = st.date_input("Début export", datetime(2024,1,1), min_value=datetime(2024,1,1), key="dl_start")
                e_dl = st.date_input("Fin export", datetime.today(), min_value=datetime(2024,1,1), key="dl_end")

                if st.button("🚀 Préparer fichier (Période)"):
                    with st.spinner("Préparation..."):
                        sync_cache(s_dl, e_dl)
                        df = load_data(s_dl, e_dl)
                        if not df.empty:
                            if format_choisi == "Excel (.xlsx)":
                                xlsx_buffer = io.BytesIO()
                                with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
                                    df.to_excel(writer, index=False, sheet_name="Meteo_PAD")
                                st.download_button("📥 Télécharger Excel", xlsx_buffer.getvalue(), "meteo_pad.xlsx")
                            else:
                                with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                                    ds = df.set_index(['DateTime', 'Station']).to_xarray()
                                    ds.to_netcdf(tmp.name, engine="h5netcdf")
                                    tmp_path = tmp.name
                                with open(tmp_path, "rb") as f:
                                    st.download_button("📥 Télécharger NetCDF", f.read(), "meteo.nc")
                                os.remove(tmp_path)
            
            with col2:
                st.markdown("### 📂 Toute la base")
                if st.button("🚀 Préparer TOUTE LA BASE"):
                    with st.spinner("Synchronisation totale (2024 -> Aujourd'hui)..."):
                        sync_cache(None, None) # None, None = 2024 à Aujourd'hui
                        df = load_data()
                        if not df.empty:
                            if format_choisi == "Excel (.xlsx)":
                                xlsx_buffer = io.BytesIO()
                                with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
                                    df.to_excel(writer, index=False, sheet_name="Meteo_Full")
                                st.download_button("📥 Télécharger TOUT (Excel)", xlsx_buffer.getvalue(), "full_history_pad.xlsx")
                            else:
                                with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                                    ds = df.set_index(['DateTime', 'Station']).to_xarray()
                                    ds.to_netcdf(tmp.name, engine="h5netcdf")
                                    tmp_path = tmp.name
                                with open(tmp_path, "rb") as f:
                                    st.download_button("📥 Télécharger TOUT (NetCDF)", f.read(), "full_history.nc")
                                os.remove(tmp_path)


with tab2:
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
                        sujet, msg_user = generer_html_email("user_approuve", d[1], d[0])
                        envoyer_email(d[3], sujet, msg_user)
                        st.success(f"Demande {d[0]} validée et email sent.")
                        st.rerun()
                    if c2.button("❌ Refuser", key=f"ref_{d[0]}"):
                        cursor.execute("UPDATE demandes SET statut='refuse' WHERE id=?", (d[0],))
                        conn.commit()
                        sujet, msg_user = generer_html_email("user_refuse", d[1], d[0])
                        envoyer_email(d[3], sujet, msg_user)
                        st.error(f"Demande {d[0]} refusée et email sent.")
                        st.rerun()
    elif password:
        st.error("Mot de passe incorrect")

