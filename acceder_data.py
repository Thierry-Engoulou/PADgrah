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

# === Route pour accéder aux données ===
@app.route("/donnees", methods=["GET"])
def get_donnees():
    station = request.args.get("station")
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    query = {}
    
    if station:
        # Robustness: look for either 'Station' or 'STATION NAME'
        query["$or"] = [{"Station": station}, {"STATION NAME": station}]
    
    if start_str or end_str:
        date_query = {}
        if start_str:
            try:
                # Expecting YYYY-MM-DD
                start_dt = datetime.strptime(start_str, "%Y-%m-%d")
                date_query["$gte"] = start_dt
            except ValueError:
                pass
        
        if end_str:
            try:
                # Include the whole end day
                end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                date_query["$lte"] = end_dt
            except ValueError:
                pass
        
        if date_query:
            query["DateTime"] = date_query

    cursor = collection.find(query).sort("DateTime", -1).skip(offset).limit(limit)
    donnees = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        if "DateTime" in doc and isinstance(doc["DateTime"], datetime):
            doc["DateTime"] = doc["DateTime"].strftime("%Y-%m-%d %H:%M:%S")
        donnees.append(doc)

    total = collection.count_documents(query)

    return jsonify({
        "total": total,
        "count": len(donnees),
        "data": donnees
    })

# === Lancer l’API ===
if __name__ == "__main__":
    app.run(debug=True, port=5000)
