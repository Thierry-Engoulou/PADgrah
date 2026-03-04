# ==========================================================
# SCRIPT IMPORT HISTORIQUE – MONGO + LISSAGE + HARMONIQUE
# VERSION PRODUCTION ROBUSTE
# ==========================================================

import os
import time
import logging
import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne, errors
from dotenv import load_dotenv
from scipy.signal import savgol_filter
from tqdm import tqdm
import certifi

# ==========================================================
# CONFIGURATION
# ==========================================================

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

coordonnees_stations = {
    "SM 1": {"Longitude": 9.4601, "Latitude": 3.8048},
    "SM 2": {"Longitude": 9.4950, "Latitude": 3.9165},
    "SM 3": {"Longitude": 9.5877, "Latitude": 3.9916},
    "SM 4": {"Longitude": 9.6857, "Latitude": 4.0539},
}

parametres = [
    "AIR TEMPERATURE","AIR PRESSURE","HUMIDITY","DEWPOINT",
    "WIND SPEED","WIND DIR","SURGE","TIDE HEIGHT"
]

plages_valides = {
    "AIR TEMPERATURE": (-2,50),
    "AIR PRESSURE": (900,1100),
    "HUMIDITY": (0,100),
    "DEWPOINT": (-60,60),
    "WIND SPEED": (0,150),
    "WIND DIR": (0,360),
    "SURGE": (1,5),
    "TIDE HEIGHT": (0,16),
}

# ==========================================================
# ANALYSE HARMONIQUE COMPLETE
# ==========================================================

def analyse_harmonique_complete(df, colonne, seuil_points_min=3):

    df_valid = df.dropna(subset=[colonne]).copy()
    nb_points = len(df_valid)

    if nb_points < seuil_points_min:
        logging.warning(f"{colonne} : trop peu de points ({nb_points})")
        return df

    t0 = df_valid.index.min()
    t = (df_valid.index - t0).total_seconds().values
    y = df_valid[colonne].values
    y_mean = np.mean(y)
    y = y - y_mean

    omega = {
        "M2": 2*np.pi/(12.42*3600),
        "S2": 2*np.pi/(12.00*3600),
        "K1": 2*np.pi/(23.93*3600),
        "O1": 2*np.pi/(25.82*3600),
    }

    X = [np.ones(len(t))]
    for w in omega.values():
        X.append(np.sin(w*t))
        X.append(np.cos(w*t))
    X = np.column_stack(X)

    coeffs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)

    t_all = (df.index - t0).total_seconds().values
    X_all = [np.ones(len(t_all))]
    for w in omega.values():
        X_all.append(np.sin(w*t_all))
        X_all.append(np.cos(w*t_all))
    X_all = np.column_stack(X_all)

    y_fit = X_all @ coeffs + y_mean
    df[colonne + "_MODELE"] = y_fit
    df[colonne] = df[colonne].where(df[colonne].notna(), df[colonne + "_MODELE"])

    return df

# ==========================================================
# LECTURE FICHIERS
# ==========================================================

def lire_fichier_param(station, param, dossier_base="."):

    nom = os.path.join(dossier_base, f"{station} {param}.txt")
    if not os.path.exists(nom):
        return pd.DataFrame()

    lignes = []
    with open(nom, "r", encoding="utf-8") as f:
        for l in f:
            if not l.startswith("Date"):
                lignes.append(l.strip())

    if not lignes:
        return pd.DataFrame()

    df = pd.DataFrame(
        [l.split("\t") for l in lignes],
        columns=["Date","Time",param,"SD"]
    )

    df = df[df[param] != "9999.999"]
    df["DateTime"] = pd.to_datetime(
        df["Date"]+" "+df["Time"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    )

    df[param] = pd.to_numeric(df[param], errors="coerce")

    return df[["DateTime",param]].dropna()

# ==========================================================
# FUSION + INTERPOLATION + LISSAGE
# ==========================================================

def fusionner_donnees_station(station, dossier_base="."):

    dfs = []

    for p in parametres:
        df_param = lire_fichier_param(station, p, dossier_base)
        logging.info(f"{station} - {p}: {len(df_param)} points")
        if not df_param.empty:
            dfs.append(df_param)

    if not dfs:
        return pd.DataFrame()

    df = dfs[0]
    for other in dfs[1:]:
        df = pd.merge(df, other, on="DateTime", how="outer")

    df.sort_values("DateTime", inplace=True)
    df.set_index("DateTime", inplace=True)

    for p in parametres:
        if p in df.columns:

            minv, maxv = plages_valides.get(p, (None,None))
            if minv is not None:
                df.loc[(df[p]<minv)|(df[p]>maxv), p] = np.nan

            if df[p].count() >= 2:
                df[p] = df[p].interpolate(method="time")

            if df[p].count() >= 3:
                window = min(11,len(df[p]))
                if window % 2 == 0:
                    window -= 1
                if window >= 3:
                    tmp = df[p].interpolate(limit_direction='both')
                    df[p] = savgol_filter(tmp, window_length=window, polyorder=2)

    if "TIDE HEIGHT" in df.columns:
        df = analyse_harmonique_complete(df,"TIDE HEIGHT")

    df["Station"] = station
    df["Longitude"] = coordonnees_stations[station]["Longitude"]
    df["Latitude"] = coordonnees_stations[station]["Latitude"]

    df.reset_index(inplace=True)
    df = df.where(pd.notnull(df), None)

    return df

# ==========================================================
# MONGODB ROBUSTE
# ==========================================================

def connexion_mongo(max_retries=5):

    for attempt in range(max_retries):
        try:
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                retryWrites=True,
                tls=True,
                tlsCAFile=certifi.where()
            )
            client.admin.command("ping")
            logging.info("Connexion MongoDB réussie")
            return client

        except errors.ServerSelectionTimeoutError:
            logging.warning(f"Tentative {attempt+1}/{max_retries} échouée")
            time.sleep(5)

    raise Exception("Impossible de se connecter à MongoDB")

def inserer_dans_mongo(df, collection, batch_size=1000):

    if df.empty:
        logging.warning("DataFrame vide")
        return

    records = df.to_dict("records")
    total = len(records)

    logging.info(f"Insertion de {total} documents")

    for i in tqdm(range(0, total, batch_size), desc="Mongo Insert"):

        batch = records[i:i+batch_size]

        ops = [
            UpdateOne(
                {"DateTime": doc["DateTime"], "Station": doc["Station"]},
                {"$set": doc},
                upsert=True
            )
            for doc in batch
        ]

        for retry in range(3):
            try:
                collection.bulk_write(ops, ordered=False)
                break
            except errors.AutoReconnect:
                logging.warning("Reconnexion MongoDB...")
                time.sleep(3)
            except Exception as e:
                logging.error(f"Erreur batch {i}: {e}")
                break

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    dossier_base = r"E:\Marine Weather Data\Data Storage\2024"

    logging.info("IMPORT HISTORIQUE DEMARRE")

    client = connexion_mongo()
    db = client["meteo_douala"]
    collection = db["donnees_meteo"]

    collection.create_index(
        [("DateTime", 1), ("Station", 1)],
        unique=True
    )

    total_global = 0

    for station in coordonnees_stations.keys():

        logging.info(f"Traitement {station}")

        df_station = fusionner_donnees_station(station, dossier_base)

        inserer_dans_mongo(df_station, collection)

        logging.info(f"{len(df_station)} points importés pour {station}")

        total_global += len(df_station)

    logging.info(f"IMPORT TERMINE | Total = {total_global}")