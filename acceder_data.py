from flask import Flask, jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client["meteo_douala"]
collection = db["donnees_meteo"]

# Initialiser Flask
app = Flask(__name__)

# === Route par défaut ===
@app.route("/")
def home():
    return jsonify({
        "message": "✅ API Météo Douala opérationnelle",
        "endpoints": ["/donnees", "/donnees?station=SM 2", "/donnees?limit=10"]
    })

import numpy as np

def scrub_nan(data):
    """Remplace les valeurs NaN par None pour la compatibilité JSON."""
    if isinstance(data, list):
        return [scrub_nan(v) for v in data]
    if isinstance(data, dict):
        return {k: scrub_nan(v) for k, v in data.items()}
    if isinstance(data, float) and np.isnan(data):
        return None
    return data

# === Route pour accéder aux données ===
@app.route("/donnees", methods=["GET"])
def get_donnees():
    station = request.args.get("station")
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    # On construit une liste de conditions pour $and
    and_conditions = []
    
    # 1. Filtre Station (Optionnel)
    if station:
        and_conditions.append({
            "$or": [{"Station": station}, {"STATION NAME": station}]
        })
    
    # 2. Filtre Date (Optionnel)
    if start_str or end_str:
        # Préparation des objets Date pour MongoDB
        date_query = {}
        if start_str:
            try: date_query["$gte"] = datetime.strptime(start_str, "%Y-%m-%d")
            except: pass
        if end_str:
            try: date_query["$lte"] = datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            except: pass
        
        # Préparation des filtres String (au cas où les dates sont stockées en Str)
        # Pour end_str, on ajoute " 23:59:59" pour couvrir toute la journée en comparaison alphabétique
        s_str = start_str if start_str else "0000"
        e_str = (end_str + " 23:59:59") if end_str else "9999"

        # Optimisation : On utilise un filtre plus spécifique pour aider MongoDB
        # On vérifie les deux types (Date et String)
        date_logic = {
            "$or": [
                {"DateTime": date_query},
                {"DateTime": {"$gte": s_str, "$lte": e_str}}
            ]
        }
        and_conditions.append(date_logic)

    # Construction finale de la requête
    query = {"$and": and_conditions} if and_conditions else {}

    try:
        # Optimisation : On ne trie pas si on demande trop de données sans index (sécurité)
        cursor = collection.find(query).sort("DateTime", -1).limit(limit).skip(offset)
        donnees = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            if "DateTime" in doc and isinstance(doc["DateTime"], datetime):
                doc["DateTime"] = doc["DateTime"].strftime("%Y-%m-%d %H:%M:%S")
            donnees.append(doc)

        total = collection.count_documents(query)
        
        # SÉCURITÉ : Si vide avec filtres, on vérifie si la base est vide
        if total == 0 and and_conditions:
            sample = collection.find_one()
            if sample:
                return jsonify({
                    "message": "Filtres trop restrictifs ou noms de champs incorrects",
                    "sample_doc_keys": list(sample.keys()),
                    "query_used": str(query)
                }), 200

        return jsonify(scrub_nan({
            "total": total,
            "count": len(donnees),
            "data": donnees
        }))
        
    except Exception as e:
        return jsonify({"error": str(e), "query": str(query)}), 500

# === Lancer l’API ===
if __name__ == "__main__":
    app.run(debug=True, port=5000)
