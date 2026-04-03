"""
Phase 3 — Modélisation et stockage dans MongoDB
Insertion des données nettoyées dans MongoDB Atlas
Remplace l'ancien modeling_mysql_v2.py
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import pandas as pd

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()

# Configuration MongoDB
# Utilise localhost pour l'exécution locale, sinon l'URI configurée
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "id_immobilier")

# Pour éviter les problèmes SSL avec Atlas en local, forcer localhost si pas dans Docker
if "mongodb+srv://" in MONGO_URI and "localhost" not in MONGO_URI:
    logger.warning("URI Atlas détectée, utilisation de localhost pour exécution locale")
    MONGO_URI = "mongodb://localhost:27017"

logger.info(f"Connexion à MongoDB: {MONGO_URI}")

# Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

def create_indexes():
    """Crée les index nécessaires pour les performances"""
    collection = db["annonces"]

    # Index sur zone (fréquent dans les filtres)
    collection.create_index("zone")

    # Index sur type_bien
    collection.create_index("type_bien")

    # Index sur periode
    collection.create_index("periode")

    # Index 2dsphere pour les requêtes géospatiales
    collection.create_index([("localisation", "2dsphere")])

    logger.info("Index MongoDB créés")

def insert_annonces_pandas(df_pandas):
    """
    Insère les annonces nettoyées dans MongoDB depuis un DataFrame pandas
    Utilise upsert pour éviter les doublons
    """
    annonces = df_pandas.to_dict('records')

    if not annonces:
        logger.warning("Aucune annonce à insérer")
        return 0, 0

    collection = db["annonces"]
    operations = []
    inserted = 0
    updated = 0

    for annonce in annonces:
        # Ajout du champ created_at si absent
        if "created_at" not in annonce:
            annonce["created_at"] = datetime.utcnow().isoformat() + "Z"

        # Nettoyer les NaN
        annonce = {k: v for k, v in annonce.items() if pd.notna(v)}

        # Filtre pour upsert (même titre + prix + zone = même annonce)
        filter_doc = {
            "titre": annonce.get("titre"),
            "prix": annonce.get("prix"),
            "zone": annonce.get("zone")
        }

        # Opération upsert
        operations.append(
            UpdateOne(filter_doc, {"$set": annonce}, upsert=True)
        )

    try:
        # Exécution en batch
        result = collection.bulk_write(operations, ordered=False)

        inserted = result.upserted_count
        updated = result.modified_count

        logger.info(f"Annonces insérées: {inserted}, mises à jour: {updated}")

    except BulkWriteError as bwe:
        logger.error(f"Erreur lors de l'insertion en batch: {bwe.details}")
        # En cas d'erreur, compter les succès partiels
        inserted = bwe.details.get('nUpserted', 0)
        updated = bwe.details.get('nModified', 0)

    return inserted, updated

def get_stats():
    """Retourne des statistiques sur la collection annonces"""
    collection = db["annonces"]
    total = collection.count_documents({})
    sources = collection.distinct("source")
    zones = collection.distinct("zone")

    return {
        "total_annonces": total,
        "sources": sources,
        "zones": zones
    }

# Création des index au premier import
if __name__ == "__main__":
    import pandas as pd
    import os
    
    # Lire les données nettoyées
    cleaned_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "cleaned_v2", "annonces_clean.csv")
    
    if os.path.exists(cleaned_file):
        logger.info(f"Lecture des données nettoyées: {cleaned_file}")
        df = pd.read_csv(cleaned_file)
        logger.info(f"Données chargées: {len(df)} annonces")
        
        # Insérer dans MongoDB
        inserted, updated = insert_annonces_pandas(df)
        logger.info(f"Insertion terminée: {inserted} insérées, {updated} mises à jour")
    else:
        logger.warning(f"Fichier de données nettoyées non trouvé: {cleaned_file}")
    
    # Créer les index
    create_indexes()
    
    # Afficher les stats
    stats = get_stats()
    logger.info(f"Statistiques MongoDB: {stats}")
