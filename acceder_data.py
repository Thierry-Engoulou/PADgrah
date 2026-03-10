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
        "endpoints": ["/donnees", "/donnees?station=SM 2", "/donnees?limit=10", "/debug"]
    })

# === Route Debug Scientifique ===
@app.route("/debug")
def debug():
    doc = collection.find_one()
    if doc:
        doc["_id"] = str(doc["_id"])
        # Vérifier le type de DateTime
        dt_val = doc.get("DateTime")
        return jsonify({
            "sample_doc": doc,
            "datetime_type": str(type(dt_val)),
            "keys": list(doc.keys())
        })
    return jsonify({"message": "Aucune donnée trouvée"})

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
    
    if station:
        and_conditions.append({
            "$or": [{"Station": station}, {"STATION NAME": station}]
        })
    
    if start_str or end_str:
        date_query = {}
        if start_str:
            try:
                date_query["$gte"] = datetime.strptime(start_str, "%Y-%m-%d")
            except: pass
        if end_str:
            try:
                date_query["$lte"] = datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            except: pass
        
        if date_query:
            # On cherche soit en tant que Date, soit en tant que String
            date_cond = {
                "$or": [
                    {"DateTime": date_query},
                    # Si c'est stocké en string, on fait une comparaison directe (si format ISO)
                    {"DateTime": {"$gte": start_str if start_str else "0000", "$lte": end_str if end_str else "9999"}}
                ]
            }
            and_conditions.append(date_cond)

    query = {"$and": and_conditions} if and_conditions else {}

    try:
        cursor = collection.find(query).sort("DateTime", -1).skip(offset).limit(limit)
        donnees = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            if "DateTime" in doc and isinstance(doc["DateTime"], datetime):
                doc["DateTime"] = doc["DateTime"].strftime("%Y-%m-%d %H:%M:%S")
            donnees.append(doc)

        total = collection.count_documents(query)
        
        # Scrub NaN values to prevent JSON errors
        response_data = {
            "total": total,
            "count": len(donnees),
            "data": donnees
        }
        return jsonify(scrub_nan(response_data))
        
    except Exception as e:
        return jsonify({"error": str(e), "query": str(query)}), 500

# === Lancer l’API ===
if __name__ == "__main__":
    app.run(debug=True, port=5000)
