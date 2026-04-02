"""
Phase 5 — Calcul de l'indice ID Immobilier
Indice = (prix_moyen_m2_periode_N / prix_moyen_m2_reference) * 100
Agrégation par zone/type_bien/periode
"""

import pandas as pd
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "id_immobilier")

if not MONGO_URI:
    raise ValueError("MONGO_URI non trouvée dans le fichier .env")

PERIODE_REFERENCE = "GLOBAL"


def get_connection():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def calculer_indice(db):
    docs = list(db.statistiques.find({}, {
        "zone": 1,
        "type_bien": 1,
        "type_offre": 1,
        "periode": 1,
        "prix_moyen_m2": 1,
        "nombre_annonces": 1
    }))

    if not docs:
        print("Aucune statistique disponible pour le calcul de l'indice")
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    if df.empty:
        return df

    df["zone"] = df["zone"].astype(str)
    df["type_bien"] = df["type_bien"].astype(str)
    df["type_offre"] = df["type_offre"].astype(str)
    df["periode"] = df["periode"].astype(str)

    if "GLOBAL" in df["periode"].unique():
        ref = df[df["periode"] == "GLOBAL"].copy()
    else:
        ref = df.sort_values("periode").groupby(["zone", "type_bien"]).first().reset_index()

    ref = ref[["zone", "type_bien", "prix_moyen_m2"]].rename(columns={"prix_moyen_m2": "prix_reference"})
    df = df.merge(ref, on=["zone", "type_bien"], how="left")

    df["indice_valeur"] = (df["prix_moyen_m2"] / df["prix_reference"] * 100).round(4)

    def tendance(indice):
        if pd.isna(indice):
            return None
        if indice > 105:
            return "HAUSSE"
        if indice < 95:
            return "BAISSE"
        return "STABLE"

    df["tendance"] = df["indice_valeur"].apply(tendance)
    return df


def inserer_indice(db, df):
    collection = db.indices
    for _, row in df.iterrows():
        if pd.isna(row.get("indice_valeur")):
            continue

        key = {
            "zone": row["zone"],
            "type_bien": row["type_bien"],
            "periode": row["periode"]
        }
        doc = {
            **key,
            "type_offre": row.get("type_offre"),
            "prix_moyen_m2": float(row["prix_moyen_m2"]),
            "prix_reference": float(row["prix_reference"]),
            "indice_valeur": float(row["indice_valeur"]),
            "tendance": row["tendance"],
            "nombre_annonces": int(row["nombre_annonces"]),
            "updated_at": pd.Timestamp.now().to_pydatetime()
        }
        collection.update_one(key, {"$set": doc}, upsert=True)

    print(f" {len(df)} indices upsertés")


def afficher_tendances(df):
    print("\n RÉSUMÉ DES TENDANCES PAR ZONE :")
    if not df.empty:
        print(df.groupby("tendance")["zone"].count().to_string())

    print("\n Zones en HAUSSE :")
    hausse = df[df["tendance"] == "HAUSSE"][ ["zone", "type_bien", "indice_valeur"]].head(5)
    print(hausse.to_string(index=False))

    print("\n Zones en BAISSE :")
    baisse = df[df["tendance"] == "BAISSE"][ ["zone", "type_bien", "indice_valeur"]].head(5)
    print(baisse.to_string(index=False))


def run():
    print(" Calcul de l'indice ID Immobilier...")
    db = get_connection()
    df = calculer_indice(db)
    if df.empty:
        print("Aucun indice calculé, arret.")
        return
    inserer_indice(db, df)
    afficher_tendances(df)
    print(" Indice calculé !")


if __name__ == "__main__":
    run()
