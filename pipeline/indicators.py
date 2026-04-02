"""
Phase 4 — Calcul des indicateurs statistiques
Prix au m², moyenne/médiane par zone, écart vs valeurs vénales officielles
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


def get_connection():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def calculer_statistiques(db):
    annonces = list(db.annonces.find({"Valeur par m2": {"$gt": 0}}, {
        "zone": 1, "type_bien": 1, "type_offre": 1, "periode": 1,
        "Valeur par m2": 1, "prix": 1
    }))

    if not annonces:
        print("Aucune annonce disponible pour le calcul des indicateurs")
        return pd.DataFrame()

    df = pd.DataFrame(annonces)
    if df.empty:
        return df

    df["zone"] = df["zone"].astype(str)
    df["type_bien"] = df["type_bien"].astype(str)
    df["type_offre"] = df["type_offre"].astype(str)
    df["periode"] = df["periode"].fillna("unknown").astype(str)

    stats = df.groupby(["zone", "type_bien", "type_offre", "periode"]).agg(
        prix_moyen_m2=("Valeur par m2", "mean"),
        prix_median_m2=("Valeur par m2", "median"),
        prix_min=("prix", "min"),
        prix_max=("prix", "max"),
        nombre_annonces=("Valeur par m2", "count")
    ).reset_index()

    stats["prix_moyen_m2"] = stats["prix_moyen_m2"].round(2)
    stats["prix_median_m2"] = stats["prix_median_m2"].round(2)

    venales = list(db.valeurs_venales.find({"prix_m2_officiel": {"$gt": 0}}, {"zone": 1, "prix_m2_officiel": 1}))
    if venales:
        df_venales = pd.DataFrame(venales)
        df_venales = df_venales.groupby("zone").agg(prix_m2_officiel=("prix_m2_officiel", "mean")).reset_index()
        stats = stats.merge(df_venales, on="zone", how="left")
    else:
        stats["prix_m2_officiel"] = None

    mask = stats["prix_m2_officiel"].notna() & (stats["prix_m2_officiel"] > 0)
    stats["ecart_valeur_venale"] = None
    stats.loc[mask, "ecart_valeur_venale"] = (
        (stats.loc[mask, "prix_moyen_m2"] - stats.loc[mask, "prix_m2_officiel"]) /
        stats.loc[mask, "prix_m2_officiel"] * 100
    ).round(2)

    return stats


def inserer_statistiques(db, stats):
    collection = db.statistiques
    for _, row in stats.iterrows():
        key = {
            "zone": row["zone"],
            "type_bien": row["type_bien"],
            "type_offre": row["type_offre"],
            "periode": row["periode"]
        }
        doc = {
            **key,
            "prix_moyen_m2": float(row["prix_moyen_m2"]),
            "prix_median_m2": float(row["prix_median_m2"]),
            "prix_min": float(row["prix_min"]),
            "prix_max": float(row["prix_max"]),
            "nombre_annonces": int(row["nombre_annonces"]),
            "prix_m2_officiel": float(row["prix_m2_officiel"]) if pd.notna(row.get("prix_m2_officiel")) else None,
            "ecart_valeur_venale": float(row["ecart_valeur_venale"]) if pd.notna(row.get("ecart_valeur_venale")) else None,
            "updated_at": pd.Timestamp.now().to_pydatetime()
        }
        collection.update_one(key, {"$set": doc}, upsert=True)

    print(f"  {len(stats)} statistiques upsertées")


def afficher_top_zones(stats):
    print("\n TOP 5 zones les plus chères (prix moyen m²) :")
    top = stats.sort_values("prix_moyen_m2", ascending=False).head(5)
    print(top[["zone", "type_bien", "prix_moyen_m2", "nombre_annonces"]].to_string(index=False))

    print("\n TOP 5 zones les moins chères :")
    bottom = stats.sort_values("prix_moyen_m2").head(5)
    print(bottom[["zone", "type_bien", "prix_moyen_m2", "nombre_annonces"]].to_string(index=False))


def run():
    print("  Calcul des indicateurs...")
    db = get_connection()
    stats = calculer_statistiques(db)
    if stats.empty:
        print("Aucune statistique calculée, arret.")
        return
    inserer_statistiques(db, stats)
    afficher_top_zones(stats)
    print("  Indicateurs calculés !")


if __name__ == "__main__":
    run()
