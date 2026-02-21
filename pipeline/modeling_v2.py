"""
Phase 3 - Modelisation v2 (version amelioree)
Differences avec modeling.py v1 :
  - Lit depuis data/cleaned_v2/ (donnees nettoyees par cleaning_v2.py)
  - Insere les donnees rejetees dans la table annonces_rejetees
  - Meilleure gestion des erreurs
"""

import pandas as pd
import mysql.connector
import os
import glob
from dotenv import load_dotenv

load_dotenv()

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned_v2")
REJETS_DIR  = os.path.join(BASE_DIR, "data", "raw", "rejets")
SOURCES_DIR = os.path.join(BASE_DIR, "data", "raw", "sources")

# Dictionnaire de standardisation (securite supplementaire)
TYPES_BIEN_STANDARD = {
    "chambre": "Chambre", "chambres": "Chambre",
    "chambre meublee": "Chambre", "chambre meublée": "Chambre",
    "studio": "Studio", "studio meuble": "Studio", "studio meublé": "Studio",
    "chambre salon": "Chambre Salon",
    "1 chambre": "Maison", "2 chambres": "Maison", "3 chambres": "Maison",
    "4 chambres": "Maison", "5 chambres": "Maison", "6 chambres": "Maison",
    "1chambre": "Maison", "2chambre": "Maison", "3chambre": "Maison",
    "1 chambre salon": "Maison", "2 chambres salon": "Maison",
    "3 chambres salon": "Maison", "4 chambres salon": "Maison",
    "maison": "Maison", "maisons": "Maison",
    "villa": "Villa", "villas": "Villa", "villa moderne": "Villa",
    "villa meublee": "Villa", "villa meublée": "Villa",
    "appartement": "Appartement", "appartements": "Appartement",
    "appartement meuble": "Appartement", "appartement meublé": "Appartement",
    "terrain": "Terrain", "terrains": "Terrain", "terrain urbain": "Terrain",
    "terrain agricole": "Terrain Agricole", "terrains agricoles": "Terrain Agricole",
    "bureau": "Bureau", "bureaux": "Bureau",
    "bureau/commerce": "Commerce", "espace commercial": "Commerce",
    "boutique": "Boutique", "boutiques": "Boutique",
    "magasin": "Magasin", "commerce": "Commerce",
    "immeuble": "Immeuble", "immeubles": "Immeuble",
    "immeuble commercial": "Immeuble",
    "entrepot": "Entrepot", "entrepôt": "Entrepot",
    "bar": "Bar", "hotel": "Hotel", "ecole": "Ecole", "station": "Station Service",
}

DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "id_immobilier"),
}


def get_connection():
    return mysql.connector.connect(**DB_CONFIG, buffered=True)


def create_schema(conn):
    schema_path = os.path.join(BASE_DIR, "sql", "schema.sql")
    cursor = conn.cursor()
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt and not stmt.startswith("--"):
            try:
                cursor.execute(stmt)
            except Exception:
                pass
    conn.commit()
    cursor.close()
    print("  Tables verifiees/creees")


def vider_tables(conn):
    """Vide les tables avant reinsertion pour eviter les doublons"""
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    # Ordre : enfants d abord, parents ensuite
    for table in ["annonce", "bien_immobilier", "valeur_venale",
                  "statistiques_zone", "indice_immobilier", "annonces_rejetees",
                  "source_donnees", "zone_geographique"]:
        cursor.execute(f"TRUNCATE TABLE {table}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    cursor.close()
    print("  Tables videes proprement")


# Mapping de standardisation des noms de sources
SOURCES_STANDARD = {
    "immoask"    : "ImmoAsk",
    "facebook"   : "Facebook",
    "coinafrique": "CoinAfrique",
    "valeursvenales": "ValeursVenales",
}

def standardiser_source(nom):
    """Retourne le nom standard d une source."""
    return SOURCES_STANDARD.get(str(nom).lower().strip(), str(nom).strip().title())

def insert_sources(conn, df):
    sources_brutes = df["source"].dropna().unique().tolist()
    sources_standard = list(set([standardiser_source(s) for s in sources_brutes]))
    # Ajouter aussi ValeursVenales si pas dans df
    if "ValeursVenales" not in sources_standard:
        sources_standard.append("ValeursVenales")
    cursor = conn.cursor()
    for nom in sources_standard:
        cursor.execute(
            "INSERT INTO source_donnees (nom, url, date_collecte) VALUES (%s, %s, %s)",
            (nom, "", "2024-01-01")
        )
    conn.commit()
    cursor.close()
    print(f"  {len(sources_standard)} sources inserees : {sources_standard}")


def insert_zones(conn, df):
    zones = [z for z in df["zone"].dropna().unique()
             if z not in ("non spécifié", "non spécifiés", "", "nan")]
    cursor = conn.cursor()
    for zone in zones:
        cursor.execute(
            "INSERT IGNORE INTO zone_geographique (nom, commune, prefecture) VALUES (%s, %s, %s)",
            (zone, "Lome", "GOLFE")
        )
    conn.commit()
    cursor.close()
    print(f"  {len(zones)} zones inserees")


def charger_references(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id, nom FROM zone_geographique")
    zones = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
    cursor.execute("SELECT id, nom FROM source_donnees")
    sources = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
    cursor.close()
    print(f"  {len(zones)} zones en memoire | {len(sources)} sources en memoire")
    return zones, sources


def insert_annonces(conn, df):
    zones_map, sources_map = charger_references(conn)
    cursor = conn.cursor()
    count = 0
    skipped = 0

    # Preparer les donnees en batch
    biens_batch = []
    annonces_data = []  # stocke les donnees pour inserer annonces apres

    for _, row in df.iterrows():
        try:
            zone_key   = str(row.get("zone", "")).lower().strip()
            source_key = str(row.get("source", "")).lower().strip()
            id_zone    = zones_map.get(zone_key)
            id_source  = sources_map.get(source_key)

            if not id_zone or not id_source:
                skipped += 1
                continue

            surface  = float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None
            prix     = float(row["prix"]) if pd.notna(row.get("prix")) else None
            pieces_raw = row.get("pieces")
            pieces   = int(float(pieces_raw)) if pd.notna(pieces_raw) and str(pieces_raw).replace(".", "").isdigit() else None
            prix_m2  = round(prix / surface, 2) if prix and surface and surface > 0 else None

            type_bien_raw = str(row.get("type_bien", "Inconnu")).strip()
            type_bien_std = TYPES_BIEN_STANDARD.get(type_bien_raw.lower(), type_bien_raw.title())
            type_offre_std = str(row.get("type_offre", "Inconnu")).strip().upper()

            biens_batch.append((type_bien_std, type_offre_std, surface, pieces, id_zone))
            annonces_data.append((str(row.get("titre", ""))[:255], prix, prix_m2, id_source))

        except Exception:
            skipped += 1
            continue

    # Insertion en batch des biens
    BATCH_SIZE = 500
    for i in range(0, len(biens_batch), BATCH_SIZE):
        batch = biens_batch[i:i+BATCH_SIZE]
        cursor.executemany(
            """INSERT INTO bien_immobilier (type_bien, type_offre, surface_m2, pieces, id_zone)
               VALUES (%s, %s, %s, %s, %s)""",
            batch
        )
        conn.commit()
        print(f"  ... {min(i+BATCH_SIZE, len(biens_batch))} biens inseres")

    # Recuperer les IDs des biens inseres
    first_bien_id = cursor.lastrowid - len(biens_batch) + 1
    bien_ids = list(range(first_bien_id, first_bien_id + len(biens_batch)))

    # Insertion en batch des annonces
    annonces_batch = [
        (titre, prix, prix_m2, bien_id, id_source)
        for (titre, prix, prix_m2, id_source), bien_id in zip(annonces_data, bien_ids)
    ]
    for i in range(0, len(annonces_batch), BATCH_SIZE):
        batch = annonces_batch[i:i+BATCH_SIZE]
        cursor.executemany(
            """INSERT INTO annonce (titre, prix, prix_m2, id_bien, id_source)
               VALUES (%s, %s, %s, %s, %s)""",
            batch
        )
        conn.commit()
        count += len(batch)

    cursor.close()
    print(f"  {count} annonces inserees | {skipped} ignorees")


def insert_valeurs_venales(conn):
    excel_path = os.path.join(SOURCES_DIR, "valeurs_venales_togo.xlsx")
    csv_path   = os.path.join(SOURCES_DIR, "valeurs_venales_togo.csv")

    if os.path.exists(excel_path):
        df = pd.read_excel(excel_path, engine="openpyxl")
    elif os.path.exists(csv_path):
        df = pd.read_csv(csv_path, encoding="utf-8")
    else:
        print("  Fichier valeurs venales introuvable")
        return

    df = df.rename(columns={
        "Préfecture": "prefecture",
        "Zone": "zone_admin",
        "Quartier": "zone",
        "Valeur vénale (FCFA)": "prix",
        "Surface (m²)": "surface_m2",
        "Valeur/m² (FCFA)": "prix_m2_officiel",
    })
    df["zone"] = df["zone"].str.lower().str.strip()
    print(f"  {len(df)} valeurs venales chargees")

    cursor = conn.cursor()
    cursor.execute("SELECT id, nom FROM zone_geographique")
    zones_map = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
    cursor.close()

    cursor = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        try:
            zone_key = str(row.get("zone", "")).lower().strip()
            id_zone  = zones_map.get(zone_key)

            if not id_zone:
                cursor.execute(
                    "INSERT IGNORE INTO zone_geographique (nom, commune, prefecture) VALUES (%s, %s, %s)",
                    (zone_key, "Lome", str(row.get("prefecture", "GOLFE")))
                )
                conn.commit()
                cursor.execute("SELECT id FROM zone_geographique WHERE nom = %s", (zone_key,))
                result = cursor.fetchone()
                id_zone = result[0] if result else None

            if not id_zone:
                continue

            cursor.execute(
                """INSERT INTO valeur_venale (id_zone, prix_m2_officiel, surface_m2, valeur_totale)
                   VALUES (%s, %s, %s, %s)""",
                (
                    id_zone,
                    float(row["prix_m2_officiel"]) if pd.notna(row.get("prix_m2_officiel")) else None,
                    float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None,
                    float(row["prix"]) if pd.notna(row.get("prix")) else None,
                )
            )
            count += 1
            if count % 50 == 0:
                conn.commit()
        except Exception:
            continue

    conn.commit()
    cursor.close()
    print(f"  {count} valeurs venales inserees")


def insert_rejets(conn):
    # Chercher d abord le fichier pandas, sinon les fichiers Spark
    rejets_pandas = os.path.join(REJETS_DIR, "annonces_rejetees.csv")
    rejets_spark  = glob.glob(os.path.join(REJETS_DIR, "annonces_rejetees", "part-*.csv"))

    if os.path.exists(rejets_pandas):
        df = pd.read_csv(rejets_pandas, low_memory=False)
    elif rejets_spark:
        df = pd.concat([pd.read_csv(f) for f in rejets_spark], ignore_index=True)
    else:
        print("  Aucun fichier de rejets trouve")
        return
    cursor = conn.cursor()
    count = 0

    for _, row in df.iterrows():
        try:
            cursor.execute(
                """INSERT INTO annonces_rejetees
                   (titre, zone, prix, surface_m2, type_bien, type_offre, source, raison_rejet)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    str(row.get("titre", ""))[:255],
                    str(row.get("zone", ""))[:255],
                    float(row["prix"]) if pd.notna(row.get("prix")) else None,
                    float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None,
                    str(row.get("type_bien", ""))[:100],
                    str(row.get("type_offre", ""))[:50],
                    str(row.get("source", ""))[:100],
                    str(row.get("raison_rejet", ""))[:100],
                )
            )
            count += 1
        except Exception:
            continue

    conn.commit()
    cursor.close()
    print(f"  {count} rejets inseres dans annonces_rejetees")


def run():
    print("Connexion a MySQL...")
    conn = get_connection()

    print("Verification du schema...")
    create_schema(conn)

    print("Vidage des tables pour reinsertion propre...")
    vider_tables(conn)

    print("Chargement des donnees nettoyees v2...")
    # Chercher d abord le fichier pandas, sinon les fichiers Spark
    annonces_pandas = os.path.join(CLEANED_DIR, "annonces_clean.csv")
    annonces_spark  = glob.glob(os.path.join(CLEANED_DIR, "annonces", "part-*.csv"))

    if os.path.exists(annonces_pandas):
        df = pd.read_csv(annonces_pandas, low_memory=False)
        print(f"  {len(df)} lignes chargees depuis annonces_clean.csv (pandas)")
    elif annonces_spark:
        df = pd.concat([pd.read_csv(f) for f in annonces_spark], ignore_index=True)
        print(f"  {len(df)} lignes chargees depuis {len(annonces_spark)} fichier(s) Spark")
    else:
        print(f"Aucun fichier trouve dans : {CLEANED_DIR}")
        print("Lance d abord : python pipeline/cleaning_v2_pandas.py")
        conn.close()
        return
    print(f"  Colonnes : {df.columns.tolist()}")
    print(f"  Sources  : {df['source'].dropna().unique().tolist()}")

    print("Insertion des sources...")
    insert_sources(conn, df)

    print("Insertion des zones...")
    insert_zones(conn, df)

    print("Insertion des biens et annonces...")
    insert_annonces(conn, df)

    print("Insertion des valeurs venales...")
    insert_valeurs_venales(conn)

    print("Insertion des donnees rejetees...")
    insert_rejets(conn)

    conn.close()
    print("\nModelisation V2 terminee !")
  

if __name__ == "__main__":
    run() 