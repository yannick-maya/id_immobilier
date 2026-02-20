"""
Phase 2 - Nettoyage V2 PANDAS (version rapide sans Spark)
Meme logique que cleaning_v2.py mais avec pandas.
Temps d execution : 5-10 secondes au lieu de 10 minutes.
"""

import pandas as pd
import numpy as np
import os
import re

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR     = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned_v2")
REJETS_DIR  = os.path.join(BASE_DIR, "data", "raw", "rejets")

os.makedirs(CLEANED_DIR, exist_ok=True)
os.makedirs(REJETS_DIR, exist_ok=True)

# ── Regles metier ──────────────────────────────────────────────────────────────
PRIX_M2_MIN  = 1_000
PRIX_M2_MAX  = 2_000_000
ZONE_MAX_LEN = 30

MOTS_SUSPECTS = [
    "pharmacie", "cote de", "juste", "derriere", "face a",
    "avant", "apres", "carrefour", "forever", "standing",
    "meuble", "cuisine", "clinique", "goudron", "pave",
    "boulevard circulaire", "non loin", "a cote"
]

ZONES_INVALIDES = ["non spécifié", "non spécifiés", "togo", "nan", "", "none", "null"]

# ── Dictionnaire de standardisation ───────────────────────────────────────────
TYPES_BIEN_STANDARD = {
    # Chambre seule
    "chambre": "Chambre",
    "chambre meublee": "Chambre",
    "chambre meublée": "Chambre",
    "chambre non meublee": "Chambre",
    "studio": "Studio",
    "studio meuble": "Studio",
    "studio meublé": "Studio",

    # Chambre salon
    "chambre salon": "Chambre Salon",

    # Maison avec N chambres -> Maison + pieces
    "1 chambre": "Maison", "2 chambres": "Maison", "3 chambres": "Maison",
    "4 chambres": "Maison", "5 chambres": "Maison", "6 chambres": "Maison",
    "1chambre": "Maison",  "2chambre": "Maison",  "3chambre": "Maison",
    "4chambre": "Maison",  "5chambre": "Maison",  "6chambre": "Maison",
    "1 chambre salon": "Maison", "2 chambres salon": "Maison",
    "3 chambres salon": "Maison", "4 chambres salon": "Maison",
    "1 chambre a coucher": "Maison", "2 chambres a coucher": "Maison",
    "3 chambres a coucher": "Maison", "4 chambres a coucher": "Maison",
    "maison": "Maison", "maisons": "Maison",

    # Villa
    "villa": "Villa", "villas": "Villa",
    "villa moderne": "Villa", "villa duplex": "Villa",
    "villa meublee": "Villa", "villa meublée": "Villa",

    # Appartement
    "appartement": "Appartement", "appartements": "Appartement",
    "appart": "Appartement",
    "appartement meuble": "Appartement", "appartement meublé": "Appartement",

    # Terrain
    "terrain": "Terrain", "terrains": "Terrain",
    "terrain urbain": "Terrain", "terrain a batir": "Terrain",
    "terrain agricole": "Terrain Agricole",
    "terrains agricoles": "Terrain Agricole",

    # Commerce / Bureau
    "bureau": "Bureau", "bureaux": "Bureau",
    "bureau/commerce": "Commerce",
    "boutique": "Boutique", "boutiques": "Boutique",
    "magasin": "Magasin", "commerce": "Commerce",
    "local commercial": "Commerce", "espace commercial": "Commerce",

    # Immeuble
    "immeuble": "Immeuble", "immeubles": "Immeuble",
    "immeuble de rapport": "Immeuble",
    "immeuble commercial": "Immeuble",

    # Entrepot
    "entrepot": "Entrepot", "entrepôt": "Entrepot",

    # Autres
    "bar": "Bar", "hotel": "Hotel",
    "ecole": "Ecole", "station": "Station Service",
}


def clean_prix(series):
    """Nettoie une colonne prix : supprime les caracteres non numeriques."""
    return pd.to_numeric(
        series.astype(str).str.replace(r"[^\d.]", "", regex=True),
        errors="coerce"
    )


def extraire_pieces(type_bien_str):
    """Extrait le chiffre devant 'chambre' dans le type de bien."""
    if pd.isna(type_bien_str):
        return None
    match = re.match(r"^(\d+)\s*chambre", str(type_bien_str).lower().strip())
    return int(match.group(1)) if match else None


def standardiser_type_bien(type_bien_str):
    """Standardise le type de bien via le dictionnaire."""
    if pd.isna(type_bien_str):
        return "Non specifie"
    key = str(type_bien_str).lower().strip()
    if key in TYPES_BIEN_STANDARD:
        return TYPES_BIEN_STANDARD[key]
    # Capitaliser si non trouvé dans le dict
    return str(type_bien_str).strip().title()


def ajouter_raison_rejet(df):
    """Applique les regles de rejet et retourne la raison."""
    df = df.copy()
    df["raison_rejet"] = None

    # Regle 1 : zone trop longue
    mask = df["zone"].astype(str).str.len() > ZONE_MAX_LEN
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "zone_trop_longue"

    # Regle 2 : zone invalide
    mask = df["zone"].astype(str).str.lower().str.strip().isin(ZONES_INVALIDES)
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "zone_invalide"

    # Regle 3 : zone contient mot suspect
    pattern = "|".join(MOTS_SUSPECTS)
    mask = df["zone"].astype(str).str.lower().str.contains(pattern, na=False)
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "zone_description_lieu"

    # Regle 4 : prix ou surface manquant
    mask = df["prix"].isna() | df["surface_m2"].isna()
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "prix_ou_surface_manquant"

    # Regle 5 : calcul prix m2 et filtrage aberrations
    df["prix_m2_calc"] = df["prix"] / df["surface_m2"].replace(0, np.nan)
    mask_eleve = df["prix_m2_calc"] > PRIX_M2_MAX
    mask_bas   = df["prix_m2_calc"] < PRIX_M2_MIN
    df.loc[mask_eleve & df["raison_rejet"].isna(), "raison_rejet"] = "prix_m2_trop_eleve"
    df.loc[mask_bas   & df["raison_rejet"].isna(), "raison_rejet"] = "prix_m2_trop_bas"

    return df


def clean_annonces(source_name):
    """Nettoie un fichier source et retourne (df_valides, df_rejetes)."""
    path = os.path.join(RAW_DIR, f"{source_name}.csv")
    if not os.path.exists(path):
        print(f"  [!] Fichier non trouve : {path}")
        return pd.DataFrame(), pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)

    # Renommage standard
    rename_map = {
        "Titre": "titre", "Type d'offre": "type_offre",
        "Type de bien": "type_bien", "Quartier": "zone",
        "Prix": "prix", "Piece": "pieces", "Surface": "surface_m2",
        "Source": "source_orig"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if "source_orig" in df.columns:
        df = df.drop(columns=["source_orig"])

    df["source"] = source_name

    # Nettoyage de base
    df["prix"]      = clean_prix(df["prix"])
    df["surface_m2"] = pd.to_numeric(df.get("surface_m2"), errors="coerce")
    df["zone"]      = df.get("zone", pd.Series()).astype(str).str.lower().str.strip()
    df["pieces"]    = pd.to_numeric(df.get("pieces"), errors="coerce")

    # Extraction pieces depuis type_bien AVANT standardisation
    if "type_bien" in df.columns:
        pieces_extraites = df["type_bien"].apply(extraire_pieces)
        df["pieces"] = df["pieces"].fillna(pieces_extraites)
        df["type_bien"] = df["type_bien"].apply(standardiser_type_bien)

    # Standardiser type_offre
    if "type_offre" in df.columns:
        df["type_offre"] = df["type_offre"].astype(str).str.upper().str.strip()

    # Appliquer les regles de rejet
    df = ajouter_raison_rejet(df)

    # Separer valides et rejetes
    df_valides = df[df["raison_rejet"].isna()].drop(columns=["raison_rejet", "prix_m2_calc"], errors="ignore")
    df_rejetes = df[df["raison_rejet"].notna()].drop(columns=["prix_m2_calc"], errors="ignore")

    # Deduplication stricte
    df_valides = df_valides.drop_duplicates(subset=["titre", "prix", "zone", "surface_m2"])

    return df_valides, df_rejetes


def clean_venales():
    """Nettoie les valeurs venales OTR."""
    path = os.path.join(RAW_DIR, "valeursvenales.csv")
    if not os.path.exists(path):
        print("  [!] valeursvenales.csv non trouve")
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)
    rename_map = {
        "Préfecture": "prefecture", "Zone": "zone_admin",
        "Quartier": "zone", "Valeur vénale (FCFA)": "prix",
        "Surface (m²)": "surface_m2", "Valeur/m² (FCFA)": "prix_m2_officiel",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["prix"]           = clean_prix(df.get("prix", pd.Series()))
    df["surface_m2"]     = pd.to_numeric(df.get("surface_m2"), errors="coerce")
    df["prix_m2_officiel"] = clean_prix(df.get("prix_m2_officiel", pd.Series()))
    df["zone"]           = df.get("zone", pd.Series()).astype(str).str.lower().str.strip()
    return df.dropna(subset=["zone", "prix_m2_officiel"])


def run():
    sources = ["immoask", "facebook", "coinafrique"]
    tous_valides = []
    tous_rejetes = []

    print("=" * 55)
    print("  CLEANING V2 PANDAS - ID Immobilier")
    print("=" * 55)

    for source in sources:
        print(f"\n[{source.upper()}]")
        df_v, df_r = clean_annonces(source)
        print(f"  Valides  : {len(df_v)}")
        print(f"  Rejetes  : {len(df_r)}")
        if len(df_r) > 0:
            print(f"  Raisons  : {df_r['raison_rejet'].value_counts().to_dict()}")
        tous_valides.append(df_v)
        tous_rejetes.append(df_r)

    # Concatener et sauvegarder
    df_all_valides = pd.concat(tous_valides, ignore_index=True)
    df_all_rejetes = pd.concat(tous_rejetes, ignore_index=True)
    df_venales     = clean_venales()

    df_all_valides.to_csv(os.path.join(CLEANED_DIR, "annonces_clean.csv"), index=False)
    df_all_rejetes.to_csv(os.path.join(REJETS_DIR,  "annonces_rejetees.csv"), index=False)
    df_venales.to_csv(os.path.join(CLEANED_DIR,     "venales_clean.csv"), index=False)

    print("\n" + "=" * 55)
    print(f"  Total valides  : {len(df_all_valides)}")
    print(f"  Total rejetes  : {len(df_all_rejetes)}")
    print(f"  Taux rejet     : {len(df_all_rejetes)/(len(df_all_valides)+len(df_all_rejetes))*100:.1f}%")
    print(f"  Valeurs venales: {len(df_venales)}")
    print("=" * 55)
    print("\n Nettoyage termine !")
    print(f"   Valides  -> {CLEANED_DIR}/annonces_clean.csv")
    print(f"   Rejetes  -> {REJETS_DIR}/annonces_rejetees.csv")


if __name__ == "__main__":
    run()