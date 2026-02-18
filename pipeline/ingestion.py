import pandas as pd
import os

# Chemins des fichiers sources
SOURCES = {
    "ImmoAsk": "data/raw/sources/donnees_immobilieres_reformate_ImmoAsk.xlsx",
    "Facebook": "data/raw/sources/Facebook_donnees_immobilieres.xlsx",
    "CoinAfrique": "data/raw/sources/TRD_Cyrille_CoinAfrique_TogoImmobilier.xlsx",
    "ValeursVenales": "data/raw/sources/valeurs_venales_togo.xlsx",
}

OUTPUT_DIR = "data/raw/"

def ingest():
    for source_name, path in SOURCES.items():
        print(f"\n Ingestion : {source_name}")
        df = pd.read_excel(path, engine="openpyxl")
        df["source"] = source_name

        # Affichage des colonnes et des 5 premières lignes
        print(f"   Colonnes : {list(df.columns)}")
        print(f"    Aperçu :\n{df.head()}")

        # Sauvegarde en CSV
        output_path = os.path.join(OUTPUT_DIR, f"{source_name.lower()}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"    Sauvegardé : {output_path}")

if __name__ == "__main__":
    ingest()
