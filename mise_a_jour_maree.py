import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient, UpdateOne
from scipy.signal import savgol_filter
from dotenv import load_dotenv
import certifi

# ===============================
# CONFIG
# ===============================

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where())
db = client["meteo_douala"]
collection = db["donnees_meteo"]

stations = ["SM 1", "SM 2", "SM 3", "SM 4"]

MIN_POINTS_HARMONIQUE = 300   # Sécurité anti-instabilité

print("🟢 Système marégraphique actif 24H/24")

# ===============================
# OUTILS
# ===============================

def lissage_savgol_safe(series, window=11, polyorder=2):
    # Supprime Inf
    series = series.replace([np.inf, -np.inf], np.nan)
    # Interpolation
    series = series.interpolate()
    # Remplissage aux extrémités
    series = series.fillna(method='bfill').fillna(method='ffill')
    # Vérifie qu’il y a assez de points
    if len(series) < window:
        return series
    return savgol_filter(series, window_length=window, polyorder=polyorder)

def moyenne_mobile(series, window=10):
    series = series.replace([np.inf, -np.inf], np.nan)
    series = series.interpolate()
    series = series.fillna(method='bfill').fillna(method='ffill')
    return series.rolling(window, center=True).mean()

def traiter_direction_vent(series):
    series = series.replace([np.inf, -np.inf], np.nan).interpolate().fillna(method='bfill').fillna(method='ffill')
    radians = np.deg2rad(series)
    sin = np.sin(radians)
    cos = np.cos(radians)

    sin_smooth = moyenne_mobile(pd.Series(sin, index=series.index))
    cos_smooth = moyenne_mobile(pd.Series(cos, index=series.index))

    angle = np.rad2deg(np.arctan2(sin_smooth, cos_smooth))
    return (angle + 360) % 360

# ===============================
# NETTOYAGE GLOBAL
# ===============================

def nettoyage(df):
    for col in df.select_dtypes(include=[np.number]).columns:

        df[f"{col}_BRUT"] = df[col]

        # Suppression outliers robustes
        q1 = df[col].quantile(0.01)
        q99 = df[col].quantile(0.99)
        df.loc[(df[col] < q1) | (df[col] > q99), col] = np.nan

        # Lissage selon type
        if col == "TIDE HEIGHT":
            df[f"{col}_CORRIGE"] = lissage_savgol_safe(df[col])
        elif col == "WIND DIR":
            df[f"{col}_CORRIGE"] = traiter_direction_vent(df[col])
        elif col == "WIND SPEED":
            df[f"{col}_CORRIGE"] = moyenne_mobile(df[col])
        else:
            df[f"{col}_CORRIGE"] = lissage_savgol_safe(df[col])

    return df

# ===============================
# ANALYSE HARMONIQUE SECURISEE
# ===============================

def maree_theorique(df):

    if "TIDE HEIGHT_CORRIGE" not in df.columns:
        return df

    df_valid = df.dropna(subset=["TIDE HEIGHT_CORRIGE"])
    n = len(df_valid)
    print(f"[DEBUG] Points marée valides : {n}")

    if n < MIN_POINTS_HARMONIQUE:
        print("⚠ Analyse harmonique ignorée (pas assez de données)")
        return df

    if df_valid["TIDE HEIGHT_CORRIGE"].std() < 0.05:
        print("⚠ Signal trop faible pour analyse")
        return df

    t0 = df_valid.index.min()
    t = (df_valid.index - t0).total_seconds().values
    y = df_valid["TIDE HEIGHT_CORRIGE"].values
    y_mean = np.mean(y)
    y = y - y_mean

    omega = {
        "M2": 2*np.pi/(12.42*3600),
        "S2": 2*np.pi/(12*3600),
        "K1": 2*np.pi/(23.93*3600),
        "O1": 2*np.pi/(25.82*3600),
    }

    X = [np.ones(len(t))]
    for w in omega.values():
        X.append(np.sin(w*t))
        X.append(np.cos(w*t))
    X = np.column_stack(X)

    coeffs, *_ = np.linalg.lstsq(X, y, rcond=None)

    # Reconstruction
    t_all = (df.index - t0).total_seconds().values
    X_all = [np.ones(len(t_all))]
    for w in omega.values():
        X_all.append(np.sin(w*t_all))
        X_all.append(np.cos(w*t_all))
    X_all = np.column_stack(X_all)

    reconstruction = X_all @ coeffs + y_mean

    # Sécurité anti explosion
    if np.max(np.abs(reconstruction)) > 10:
        print("⚠ Résultat harmonique instable ignoré")
        return df

    df["TIDE_HEIGHT_THEORIQUE"] = reconstruction
    print("🌊 Analyse harmonique validée")

    return df

# ===============================
# TRAITEMENT PRINCIPAL
# ===============================

for station in stations:

    docs = list(collection.find(
        {"Station": station},
        {"_id": 0}
    ))

    if not docs:
        print(f"[DEBUG] {station} : aucune donnée")
        continue

    df = pd.DataFrame(docs)
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df.set_index("DateTime", inplace=True)
    df.sort_index(inplace=True)

    # Debug colonnes principales
    for col in ["AIR TEMPERATURE","AIR PRESSURE","HUMIDITY",
                "DEWPOINT","WIND SPEED","WIND DIR",
                "SURGE","TIDE HEIGHT"]:
        if col in df.columns:
            print(f"[DEBUG] {station} - {col}: {df[col].count()} points valides")

    df = nettoyage(df)
    df = maree_theorique(df)

    # ===============================
    # UPDATE MONGO
    # ===============================

    ops = []

    for date, row in df.iterrows():
        update_fields = {}
        for col in df.columns:
            if "_BRUT" in col or "_CORRIGE" in col or "_THEORIQUE" in col:
                if pd.notna(row[col]):
                    update_fields[col] = float(row[col])
        if update_fields:
            ops.append(UpdateOne(
                {"Station": station, "DateTime": date},
                {"$set": update_fields},
                upsert=False
            ))

    if ops:
        collection.bulk_write(ops)
        print(f"✅ {station} MongoDB enrichi")

print("🎯 Système côtier stable actif.")
