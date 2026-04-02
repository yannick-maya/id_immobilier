"""
test_mongo.py
Tester la connexion MongoDB Atlas avant de modifier le pipeline.
Lancer avec : python test_mongo.py
"""
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB", "id_immobilier")

if not uri:
    print("ERREUR : MONGO_URI introuvable dans le .env")
    print("Verifier que le fichier .env existe a la racine du projet")
    exit(1)

print(f"Tentative de connexion a MongoDB...")
print(f"Base de donnees cible : {db_name}")

try:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.server_info()
    db = client[db_name]

    print(f"\nConnexion reussie !")
    print(f"Collections existantes : {db.list_collection_names()}")

    # Test insertion
    result = db["test_connexion"].insert_one({
        "message": "connexion ok",
        "projet":  "id_immobilier"
    })
    print(f"Document insere avec id : {result.inserted_id}")

    # Nettoyage
    db["test_connexion"].drop()
    print("Collection de test supprimee - tout est propre")

    client.close()
    print("\nTout est OK — pret pour la migration MongoDB !")

except Exception as e:
    print(f"\nERREUR de connexion : {e}")
    print("\nVerifier :")
    print("  1. L'URI dans .env est correcte")
    print("  2. Le mot de passe dans l'URI est correct")
    print("  3. L'IP est autorisee sur Atlas (Network Access)")
    print("  4. Le cluster est bien demarre sur Atlas")