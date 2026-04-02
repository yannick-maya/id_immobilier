"""
Phase 2 - Nettoyage V2 PANDAS
Meme logique que la version precedente + support des CSV scrapes (immoask_scraped).
Temps d execution : 5-10 secondes.
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
ZONE_MAX_LEN = 30

PRIX_MIN = 10_000           # FCFA — en dessous : erreur de saisie
PRIX_MAX = 5_000_000_000    # FCFA — au dessus : aberrant (5 milliards)

MOTS_SUSPECTS = [
    "pharmacie", "cote de", "juste", "derriere", "face a",
    "avant", "apres", "carrefour", "forever", "standing",
    "meuble", "cuisine", "clinique", "goudron", "pave",
    "boulevard circulaire", "non loin", "a cote"
]

ZONES_INVALIDES = ["non spécifié", "non spécifiés", "togo", "nan", "", "none", "null"]

# ── Dictionnaire de standardisation ───────────────────────────────────────────
TYPES_BIEN_STANDARD = {
    "chambre": "Chambre", "chambre meublee": "Chambre", "chambre meublée": "Chambre",
    "chambre non meublee": "Chambre", "studio": "Studio",
    "studio meuble": "Studio", "studio meublé": "Studio",
    "chambre salon": "Chambre Salon",
    "1 chambre": "Maison", "2 chambres": "Maison", "3 chambres": "Maison",
    "4 chambres": "Maison", "5 chambres": "Maison", "6 chambres": "Maison",
    "1chambre": "Maison", "2chambre": "Maison", "3chambre": "Maison",
    "4chambre": "Maison", "5chambre": "Maison", "6chambre": "Maison",
    "1 chambre salon": "Maison", "2 chambres salon": "Maison",
    "3 chambres salon": "Maison", "4 chambres salon": "Maison",
    "1 chambre a coucher": "Maison", "2 chambres a coucher": "Maison",
    "3 chambres a coucher": "Maison", "4 chambres a coucher": "Maison",
    "maison": "Maison", "maisons": "Maison",
    "villa": "Villa", "villas": "Villa", "villa moderne": "Villa",
    "villa duplex": "Villa", "villa meublee": "Villa", "villa meublée": "Villa",
    "appartement": "Appartement", "appartements": "Appartement", "appart": "Appartement",
    "appartement meuble": "Appartement", "appartement meublé": "Appartement",
    "terrain": "Terrain", "terrains": "Terrain", "terrain urbain": "Terrain",
    "terrain a batir": "Terrain", "terrain agricole": "Terrain Agricole",
    "terrains agricoles": "Terrain Agricole",
    "bureau": "Bureau", "bureaux": "Bureau", "bureau/commerce": "Commerce",
    "boutique": "Boutique", "boutiques": "Boutique",
    "magasin": "Magasin", "commerce": "Commerce",
    "local commercial": "Commerce", "espace commercial": "Commerce",
    "immeuble": "Immeuble", "immeubles": "Immeuble",
    "immeuble de rapport": "Immeuble", "immeuble commercial": "Immeuble",
    "entrepot": "Entrepot", "entrepôt": "Entrepot",
    "bar": "Bar", "hotel": "Hotel", "ecole": "Ecole", "station": "Station Service",
}

# ── Mapping colonnes brutes API → colonnes pipeline ───────────────────────────
# Pour les CSV scrapés dont les colonnes ne sont pas encore au format Excel
SCRAPED_RENAME = {
    "immoask_scraped": {
        "titre"       : "Titre",
        "offre"       : "Type d'offre",
        "categorie"   : "Type de bien",
        "quartier"    : "Quartier",
        "piece"       : "Piece",
        "surface"     : "Surface",
        # Prix : fusionner cout_mensuel et cout_vente en une seule colonne Prix
        # (géré dans normalise_scraped_immoask)
    }
}


# ── Normalisation spécifique par source scrapée ───────────────────────────────

def normalise_scraped_immoask(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ramène les colonnes brutes API ImmoAsk au format attendu par clean_annonces().
    Colonnes brutes  : source, id, nuo, titre, usage, offre, categorie, ville,
                       quartier, piece, surface, garage, cout_mensuel, cout_vente, nuitee
    Colonnes cibles  : Titre, Type d'offre, Type de bien, Quartier, Prix, Piece, Surface
    """
    df = df.copy()

    # Type d'offre : louer → LOCATION, vendre → VENTE, bailler → BAILLER
    offre_map = {
        "louer": "LOCATION", "location": "LOCATION",
        "vendre": "VENTE",   "vente": "VENTE",
        "bailler": "BAILLER","bail": "BAILLER",
    }
    df["Type d'offre"] = (
        df["offre"].astype(str).str.strip().str.lower()
        .map(offre_map)
        .fillna(df["offre"].astype(str).str.upper())
    )

    # Prix : LOCATION/BAILLER → cout_mensuel, VENTE → cout_vente
    cout_mensuel = pd.to_numeric(df.get("cout_mensuel"), errors="coerce")
    cout_vente   = pd.to_numeric(df.get("cout_vente"),   errors="coerce")
    is_vente     = df["Type d'offre"] == "VENTE"
    df["Prix"]   = np.where(is_vente, cout_vente, cout_mensuel)

    # Renommage simple
    df = df.rename(columns={
        "titre"    : "Titre",
        "categorie": "Type de bien",
        "quartier" : "Quartier",
        "piece"    : "Piece",
        "surface"  : "Surface",
    })

    # Garder uniquement les colonnes pipeline + source
    cols_a_garder = ["Titre", "Type d'offre", "Type de bien", "Quartier", "Prix", "Piece", "Surface", "source"]
    df = df[[c for c in cols_a_garder if c in df.columns]]

    return df


def normalise_scraped_coinafrique(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ramène les colonnes brutes CoinAfrique au format attendu par clean_annonces().
    Colonnes brutes  : source, reference, titre, offre, type_bien, quartier,
                       ville, prix, piece, surface, url
    Note : quartier et prix sont déjà propres (extraits dans le scraper).
    """
    df = df.copy()

    # Type d'offre : vendre → VENTE, louer → LOCATION
    offre_map = {
        "louer": "LOCATION", "location": "LOCATION",
        "vendre": "VENTE",   "vente": "VENTE",
    }
    # Depuis colonne offre
    offre_from_col = df["offre"].astype(str).str.strip().str.lower().map(offre_map)
    # Fallback depuis le titre
    titre_lower = df["titre"].astype(str).str.lower()
    offre_from_titre = pd.Series("NON SPECIFIE", index=df.index)
    offre_from_titre = offre_from_titre.where(~titre_lower.str.contains("vente|vendre"), "VENTE")
    offre_from_titre = offre_from_titre.where(~titre_lower.str.contains("location|louer"), "LOCATION")
    df["Type d'offre"] = offre_from_col.fillna(offre_from_titre)

    # Quartier : déjà propre depuis le scraper
    df["Quartier"] = df["quartier"].fillna("Non spécifié")

    # Prix : déjà un entier depuis le scraper
    df["Prix"] = pd.to_numeric(df["prix"], errors="coerce")

    # Renommage
    df = df.rename(columns={
        "titre"    : "Titre",
        "type_bien": "Type de bien",
        "piece"    : "Piece",
        "surface"  : "Surface",
    })

    cols_a_garder = ["Titre", "Type d'offre", "Type de bien", "Quartier", "Prix", "Piece", "Surface", "source"]
    df = df[[c for c in cols_a_garder if c in df.columns]]
    return df


# Registre des fonctions de normalisation par source scrapée
SCRAPED_NORMALIZERS = {
    "immoask_scraped"    : normalise_scraped_immoask,
    "coinafrique_scraped": normalise_scraped_coinafrique,
    # Ajouter ici facebook_scraped, etc.
}


# ── Fonctions de nettoyage ─────────────────────────────────────────────────────

def clean_prix(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r"[^\d.]", "", regex=True),
        errors="coerce"
    )


def extraire_pieces(type_bien_str):
    if pd.isna(type_bien_str):
        return None
    match = re.match(r"^(\d+)\s*chambre", str(type_bien_str).lower().strip())
    return int(match.group(1)) if match else None


def standardiser_type_bien(type_bien_str):
    if pd.isna(type_bien_str):
        return "Non specifie"
    key = str(type_bien_str).lower().strip()
    if key in TYPES_BIEN_STANDARD:
        return TYPES_BIEN_STANDARD[key]
    return str(type_bien_str).strip().title()


def ajouter_raison_rejet(df):
    df = df.copy()
    df["raison_rejet"] = None

    mask = df["zone"].astype(str).str.len() > ZONE_MAX_LEN
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "zone_trop_longue"

    mask = df["zone"].astype(str).str.lower().str.strip().isin(ZONES_INVALIDES)
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "zone_invalide"

    pattern = "|".join(MOTS_SUSPECTS)
    mask = df["zone"].astype(str).str.lower().str.contains(pattern, na=False)
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "zone_description_lieu"

    mask = df["prix"].isna() | df["surface_m2"].isna()
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "prix_ou_surface_manquant"

    # Regle 5 : prix total aberrant
    mask = df["prix"] < PRIX_MIN
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "prix_trop_bas"

    mask = df["prix"] > PRIX_MAX
    df.loc[mask & df["raison_rejet"].isna(), "raison_rejet"] = "prix_trop_eleve"

    return df


def clean_annonces(source_name):
    """Nettoie un fichier source et retourne (df_valides, df_rejetes)."""
    path = os.path.join(RAW_DIR, f"{source_name}.csv")
    if not os.path.exists(path):
        print(f"  [!] Fichier non trouve : {path}")
        return pd.DataFrame(), pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)
    df["source"] = source_name

    # ── Normalisation spécifique si source scrapée ────────────────────────────
    if source_name in SCRAPED_NORMALIZERS:
        print(f"  [i] Normalisation colonnes brutes API pour : {source_name}")
        df = SCRAPED_NORMALIZERS[source_name](df)

    # ── Renommage standard (commun à toutes les sources) ──────────────────────
    rename_map = {
        "Titre"       : "titre",
        "Type d'offre": "type_offre",
        "Type de bien": "type_bien",
        "Quartier"    : "zone",
        "Prix"        : "prix",
        "Piece"       : "pieces",
        "Surface"     : "surface_m2",
        "Source"      : "source_orig",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if "source_orig" in df.columns:
        df = df.drop(columns=["source_orig"])

    # ── Nettoyage de base ─────────────────────────────────────────────────────
    df["prix"]       = clean_prix(df["prix"])
    df["surface_m2"] = pd.to_numeric(df.get("surface_m2"), errors="coerce")
    df["zone"]       = df.get("zone", pd.Series()).astype(str).str.lower().str.strip()
    df["pieces"]     = pd.to_numeric(df.get("pieces"), errors="coerce")

    if "type_bien" in df.columns:
        pieces_extraites = df["type_bien"].apply(extraire_pieces)
        df["pieces"]     = df["pieces"].fillna(pieces_extraites).infer_objects(copy=False)
        df["type_bien"]  = df["type_bien"].apply(standardiser_type_bien)

    if "type_offre" in df.columns:
        df["type_offre"] = df["type_offre"].astype(str).str.upper().str.strip()

    # ── Règles de rejet ───────────────────────────────────────────────────────
    df = ajouter_raison_rejet(df)

    df_valides = df[df["raison_rejet"].isna()].drop(columns=["raison_rejet"], errors="ignore")
    df_rejetes = df[df["raison_rejet"].notna()]

    df_valides = df_valides.drop_duplicates(subset=["titre", "prix", "zone", "surface_m2"])

    return df_valides, df_rejetes


def clean_venales():
    path = os.path.join(RAW_DIR, "valeursvenales.csv")
    if not os.path.exists(path):
        print("  [!] valeursvenales.csv non trouve")
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)
    rename_map = {
        "Préfecture"         : "prefecture",
        "Zone"               : "zone_admin",
        "Quartier"           : "zone",
        "Valeur vénale (FCFA)": "prix",
        "Surface (m²)"       : "surface_m2",
        "Valeur/m² (FCFA)"   : "prix_m2_officiel",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["prix"] = clean_prix(df["prix"])
    df["surface_m2"] = pd.to_numeric(df["surface_m2"], errors="coerce")
    df["prix_m2_officiel"] = pd.to_numeric(df["prix_m2_officiel"], errors="coerce")
    df["zone"] = df["zone"].astype(str).str.lower().str.strip()
    return df


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
    df["prix"]             = clean_prix(df.get("prix", pd.Series()))
    df["surface_m2"]       = pd.to_numeric(df.get("surface_m2"), errors="coerce")
    df["prix_m2_officiel"] = clean_prix(df.get("prix_m2_officiel", pd.Series()))
    df["zone"]             = df.get("zone", pd.Series()).astype(str).str.lower().str.strip()
    return df.dropna(subset=["zone", "prix_m2_officiel"])


def run():
    # Excel statiques + source scrapée ImmoAsk
    sources = ["immoask", "facebook", "coinafrique", "immoask_scraped", "coinafrique_scraped"]

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

    df_all_valides = pd.concat(tous_valides, ignore_index=True)
    df_all_rejetes = pd.concat(tous_rejetes, ignore_index=True)
    df_venales     = clean_venales()

    # Ajouter les champs de période
    df_all_valides = enrich_period_fields(df_all_valides)

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