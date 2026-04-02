"""
Phase 1 - Ingestion des donnees
Lecture des 4 sources Excel + CSV scrapés et sauvegarde en CSV dans data/raw/
"""

import pandas as pd
import os
from typing import Optional
import glob

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES_DIR  = os.path.join(BASE_DIR, "data", "raw", "sources")
SCRAPED_DIR  = os.path.join(BASE_DIR, "data", "raw", "scraped")
OUTPUT_DIR   = os.path.join(BASE_DIR, "data", "raw")

# Sources Excel statiques (fichiers collaborateurs)
SOURCES = {
    "immoask"       : "ImmoAsk.xlsx",
    "facebook"      : "Facebook_MarketPlace.xlsx",
    "coinafrique"   : "CoinAfrique_TogoImmobilier.xlsx",
    "valeursvenales": "Otr_Valeur_Venale.xlsx",
}

# Sources scrapées : nom_source → pattern de fichiers dans scraped/
SCRAPED_SOURCES = {
    "immoask_scraped"    : "immoask_*.csv",
    "coinafrique_scraped": "coinafrique_*.csv",
}


def get_latest_file(pattern: str) -> Optional[str]:
    """Retourne le fichier le plus récent correspondant au pattern."""
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def ingest_excel():
    """Ingestion des fichiers Excel statiques (inchangé)."""
    for source_name, filename in SOURCES.items():
        print(f"\n Ingestion Excel : {source_name}")

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


def ingest_scraped():
    """Ingestion des CSV scrapés : prend le fichier le plus récent pour chaque source."""
    if not os.path.exists(SCRAPED_DIR):
        print(f"\n Dossier scraped introuvable : {SCRAPED_DIR} — skip")
        return

    for source_name, pattern in SCRAPED_SOURCES.items():
        print(f"\n Ingestion scrapée : {source_name}")

        latest = get_latest_file(os.path.join(SCRAPED_DIR, pattern))

        if not latest:
            print(f"   Aucun fichier trouvé pour le pattern : {pattern}")
            print(f"   Lance d'abord : python scraper_immoask.py")
            continue

        print(f"   Fichier sélectionné : {os.path.basename(latest)}")

        df = pd.read_csv(latest, encoding="utf-8-sig")
        df["source"] = source_name

        output_path = os.path.join(OUTPUT_DIR, f"{source_name.lower()}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")

        print(f"   {len(df)} lignes | {len(df.columns)} colonnes")
        print(f"   Sauvegarde : {output_path}")


def enrich_period_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute ou normalise les colonnes periode, annee, trimestre d'après date_annonce."""

    # S'assurer que la colonne existe pour les opérations suivantes
    if "date_annonce" not in df.columns:
        df["date_annonce"] = pd.NaT
        print("  [!] date_annonce absent, on crée une colonne date_annonce vide")

    df["date_annonce"] = pd.to_datetime(df["date_annonce"], errors="coerce")

    annee_courante = pd.Timestamp.now().year
    trimestre_courant = pd.Timestamp.now().quarter

    df["annee"] = df.get("annee")
    df["trimestre"] = df.get("trimestre")

    df["annee"] = df["date_annonce"].dt.year.fillna(annee_courante).astype(int)
    df["trimestre"] = df["date_annonce"].dt.quarter.fillna(trimestre_courant).astype(int)
    df["periode"] = df["annee"].astype(str) + "-Q" + df["trimestre"].astype(str)

    return df


def ingest():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ingest_excel()
    ingest_scraped()

    # Concaténation de toutes les sources ingérées
    source_files = glob.glob(os.path.join(OUTPUT_DIR, "*.csv"))
    if not source_files:
        print("Aucun fichier source trouvé pour concaténation.")
        return

    frames = []
    for path in source_files:
        try:
            tmp = pd.read_csv(path, encoding="utf-8")
            frames.append(tmp)
        except Exception as ex:
            print(f"  [!] Impossible de lire {path} : {ex}")

    if not frames:
        print("Aucune donnée à concaténer après lecture des CSV.")
        return

    df_all = pd.concat(frames, ignore_index=True, sort=False)
    df_all = enrich_period_fields(df_all)

    output_combined = os.path.join(OUTPUT_DIR, "annonces.csv")
    df_all.to_csv(output_combined, index=False, encoding="utf-8")

    print(f"Concaténation terminée : {len(df_all)} lignes -> {output_combined}")
    print(f"Colonnes disponibles: {list(df_all.columns)}")


if __name__ == "__main__":
    print(f"Racine du projet : {BASE_DIR}")
    print(f"Dossier sources  : {SOURCES_DIR}")
    print(f"Dossier scraped  : {SCRAPED_DIR}")
    ingest()
    print("\nIngestion terminee !")