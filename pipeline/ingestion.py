"""
Phase 1 - Ingestion des donnees
Lecture des 4 sources Excel et sauvegarde en CSV dans data/raw/
"""

import pandas as pd
import os

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES_DIR = os.path.join(BASE_DIR, "data", "raw", "sources")
OUTPUT_DIR  = os.path.join(BASE_DIR, "data", "raw")

SOURCES = {
    "immoask":        "ImmoAsk.xlsx",
    "facebook":       "Facebook_MarketPlace.xlsx",
    "coinafrique":    "CoinAfrique_TogoImmobilier.xlsx",
    "valeursvenales": "Otr_Valeur_Venale.xlsx",
}


def ingest():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for source_name, filename in SOURCES.items():
        print(f"\n Ingestion : {source_name}")

        path = os.path.join(SOURCES_DIR, filename)

        if not os.path.exists(path):
            print(f"   Fichier introuvable : {path}")
            print(f"   Place le fichier '{filename}' dans : {SOURCES_DIR}")
            continue

        df = pd.read_excel(path, engine="openpyxl")
        df["source"] = source_name

        output_path = os.path.join(OUTPUT_DIR, f"{source_name.lower()}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")

        print(f"   {len(df)} lignes | {len(df.columns)} colonnes")
        print(f"   Sauvegarde : {output_path}")


if __name__ == "__main__":
    print(f"Racine du projet : {BASE_DIR}")
    print(f"Dossier sources  : {SOURCES_DIR}")
    ingest()
    print("\nIngestion terminee !")