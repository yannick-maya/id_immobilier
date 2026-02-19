# """
# Phase 3 — Modélisation et stockage dans MySQL
# Insertion des données nettoyées dans les tables relationnelles
# """
# """

# """

# import pandas as pd
# import mysql.connector
# import os
# import glob
# from dotenv import load_dotenv

# load_dotenv()

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")

# DB_CONFIG = {
#     "host": os.getenv("MYSQL_HOST", "localhost"),
#     "user": os.getenv("MYSQL_USER", "root"),
#     "password": os.getenv("MYSQL_PASSWORD", ""),
#     "database": os.getenv("MYSQL_DB", "id_immobilier"),
# }


# def get_connection():
#     return mysql.connector.connect(**DB_CONFIG, buffered=True)


# def create_schema(conn):
#     schema_path = os.path.join(BASE_DIR, "sql", "schema.sql")
#     cursor = conn.cursor()
#     with open(schema_path, "r", encoding="utf-8") as f:
#         sql = f.read()
#     for statement in sql.split(";"):
#         stmt = statement.strip()
#         if stmt and not stmt.startswith("--"):
#             try:
#                 cursor.execute(stmt)
#             except Exception:
#                 pass
#     conn.commit()
#     cursor.close()
#     print(" Tables vérifiées/créées")


# def insert_sources(conn, df):
#     """Insère les sources détectées directement depuis le DataFrame"""
#     sources_reelles = df["source"].dropna().unique().tolist()
#     cursor = conn.cursor()
#     for nom in sources_reelles:
#         cursor.execute(
#             "INSERT IGNORE INTO source_donnees (nom, url, date_collecte) VALUES (%s, %s, %s)",
#             (nom, "", "2024-01-01")
#         )
#     conn.commit()
#     cursor.close()
#     print(f" {len(sources_reelles)} sources insérées : {sources_reelles}")


# def insert_zones(conn, df):
#     zones = df["zone"].dropna().unique()
#     # Filtrer les zones non pertinentes
#     zones = [z for z in zones if z not in ("non spécifié", "non spécifiés", "", "nan")]
#     cursor = conn.cursor()
#     for zone in zones:
#         cursor.execute(
#             "INSERT IGNORE INTO zone_geographique (nom, commune, prefecture) VALUES (%s, %s, %s)",
#             (zone, "Lomé", "GOLFE")
#         )
#     conn.commit()
#     cursor.close()
#     print(f" {len(zones)} zones insérées")


# def charger_references(conn):
#     """Charge zones et sources en mémoire pour éviter les SELECT en boucle"""
#     cursor = conn.cursor()
#     cursor.execute("SELECT id, nom FROM zone_geographique")
#     zones = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
#     cursor.execute("SELECT id, nom FROM source_donnees")
#     sources = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
#     cursor.close()
#     print(f"    {len(zones)} zones en mémoire | {len(sources)} sources en mémoire")
#     return zones, sources


# def insert_annonces(conn, df):
#     zones_map, sources_map = charger_references(conn)

#     # Diagnostic — affiche ce qui ne matche pas
#     sources_df = df["source"].dropna().str.lower().str.strip().unique()
#     zones_df = df["zone"].dropna().str.lower().str.strip().unique()
#     sources_manquantes = [s for s in sources_df if s not in sources_map]
#     zones_manquantes = [z for z in zones_df if z not in zones_map][:5]
#     if sources_manquantes:
#         print(f"     Sources non trouvées en MySQL : {sources_manquantes}")
#     if zones_manquantes:
#         print(f"     Zones non trouvées (exemples) : {zones_manquantes}")

#     cursor = conn.cursor()
#     count = 0
#     skipped = 0

#     for _, row in df.iterrows():
#         try:
#             zone_key = str(row.get("zone", "")).lower().strip()
#             source_key = str(row.get("source", "")).lower().strip()

#             id_zone = zones_map.get(zone_key)
#             id_source = sources_map.get(source_key)

#             if not id_zone or not id_source:
#                 skipped += 1
#                 continue

#             surface = float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None
#             prix = float(row["prix"]) if pd.notna(row.get("prix")) else None
#             pieces_raw = row.get("pieces")
#             pieces = int(float(pieces_raw)) if pd.notna(pieces_raw) and str(pieces_raw).replace('.','').isdigit() else None
#             prix_m2 = round(prix / surface, 2) if prix and surface and surface > 0 else None

#             cursor.execute(
#                 """INSERT INTO bien_immobilier (type_bien, type_offre, surface_m2, pieces, id_zone)
#                    VALUES (%s, %s, %s, %s, %s)""",
#                 (str(row.get("type_bien", "Inconnu")), str(row.get("type_offre", "Inconnu")),
#                  surface, pieces, id_zone)
#             )
#             id_bien = cursor.lastrowid

#             cursor.execute(
#                 """INSERT INTO annonce (titre, prix, prix_m2, id_bien, id_source)
#                    VALUES (%s, %s, %s, %s, %s)""",
#                 (str(row.get("titre", ""))[:255], prix, prix_m2, id_bien, id_source)
#             )
#             count += 1

#             if count % 100 == 0:
#                 conn.commit()
#                 print(f"   ... {count} lignes insérées")

#         except Exception as e:
#             skipped += 1
#             continue

#     conn.commit()
#     cursor.close()
#     print(f" {count} annonces insérées | {skipped} ignorées")


# def run():
#     print("  Connexion à MySQL...")
#     conn = get_connection()

#     print(" Vérification du schéma...")
#     create_schema(conn)

#     print(" Chargement des données nettoyées...")
#     annonces_files = glob.glob(os.path.join(CLEANED_DIR, "annonces", "part-*.csv"))
#     if not annonces_files:
#         print(f" Aucun fichier trouvé dans : {os.path.join(CLEANED_DIR, 'annonces')}")
#         print(" Lance d'abord : spark-submit pipeline/cleaning.py")
#         conn.close()
#         return

#     df = pd.concat([pd.read_csv(f) for f in annonces_files], ignore_index=True)
#     print(f"    {len(df)} lignes chargées")

#     # Diagnostic colonnes
#     print(f"    Colonnes disponibles : {df.columns.tolist()}")
#     print(f"    Valeurs 'source' : {df['source'].dropna().unique().tolist()}")
#     print(f"   Exemples 'zone'  : {df['zone'].dropna().unique()[:5].tolist()}")

#     print("\n Insertion des sources...")
#     insert_sources(conn, df)

#     print(" Insertion des zones...")
#     insert_zones(conn, df)

#     print(" Insertion des biens et annonces...")
#     insert_annonces(conn, df)

#     conn.close()
#     print("\n Modélisation terminée !")


# if __name__ == "__main__":
#     run()

"""
Phase 3 — Modélisation et stockage dans MySQL
"""

import pandas as pd
import mysql.connector
import os
import glob
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
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
    print(" Tables vérifiées/créées")


def insert_sources(conn, df):
    """Insère les sources détectées directement depuis le DataFrame"""
    sources_reelles = df["source"].dropna().unique().tolist()
    cursor = conn.cursor()
    for nom in sources_reelles:
        cursor.execute(
            "INSERT IGNORE INTO source_donnees (nom, url, date_collecte) VALUES (%s, %s, %s)",
            (nom, "", "2024-01-01")
        )
    conn.commit()
    cursor.close()
    print(f" {len(sources_reelles)} sources insérées : {sources_reelles}")


def insert_zones(conn, df):
    zones = df["zone"].dropna().unique()
    # Filtrer les zones non pertinentes
    zones = [z for z in zones if z not in ("non spécifié", "non spécifiés", "", "nan")]
    cursor = conn.cursor()
    for zone in zones:
        cursor.execute(
            "INSERT IGNORE INTO zone_geographique (nom, commune, prefecture) VALUES (%s, %s, %s)",
            (zone, "Lomé", "GOLFE")
        )
    conn.commit()
    cursor.close()
    print(f" {len(zones)} zones insérées")


def charger_references(conn):
    """Charge zones et sources en mémoire pour éviter les SELECT en boucle"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, nom FROM zone_geographique")
    zones = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
    cursor.execute("SELECT id, nom FROM source_donnees")
    sources = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
    cursor.close()
    print(f"   📋 {len(zones)} zones en mémoire | {len(sources)} sources en mémoire")
    return zones, sources


def insert_annonces(conn, df):

    # Vide les tables dépendantes avant réinsertion
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE annonce")
    cursor.execute("TRUNCATE TABLE bien_immobilier")
    cursor.execute("TRUNCATE TABLE valeur_venale")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    cursor.close()
    print("     Tables annonce/bien_immobilier/valeur_venale vidées")
    # ... suite du code

    zones_map, sources_map = charger_references(conn)

    # Diagnostic — affiche ce qui ne matche pas
    sources_df = df["source"].dropna().str.lower().str.strip().unique()
    zones_df = df["zone"].dropna().str.lower().str.strip().unique()
    sources_manquantes = [s for s in sources_df if s not in sources_map]
    zones_manquantes = [z for z in zones_df if z not in zones_map][:5]
    if sources_manquantes:
        print(f"     Sources non trouvées en MySQL : {sources_manquantes}")
    if zones_manquantes:
        print(f"     Zones non trouvées (exemples) : {zones_manquantes}")

    cursor = conn.cursor()
    count = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            zone_key = str(row.get("zone", "")).lower().strip()
            source_key = str(row.get("source", "")).lower().strip()

            id_zone = zones_map.get(zone_key)
            id_source = sources_map.get(source_key)

            if not id_zone or not id_source:
                skipped += 1
                continue

            surface = float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None
            prix = float(row["prix"]) if pd.notna(row.get("prix")) else None
            pieces_raw = row.get("pieces")
            pieces = int(float(pieces_raw)) if pd.notna(pieces_raw) and str(pieces_raw).replace('.','').isdigit() else None
            prix_m2 = round(prix / surface, 2) if prix and surface and surface > 0 else None

            cursor.execute(
                """INSERT INTO bien_immobilier (type_bien, type_offre, surface_m2, pieces, id_zone)
                   VALUES (%s, %s, %s, %s, %s)""",
                (str(row.get("type_bien", "Inconnu")), str(row.get("type_offre", "Inconnu")),
                 surface, pieces, id_zone)
            )
            id_bien = cursor.lastrowid

            cursor.execute(
                """INSERT INTO annonce (titre, prix, prix_m2, id_bien, id_source)
                   VALUES (%s, %s, %s, %s, %s)""",
                (str(row.get("titre", ""))[:255], prix, prix_m2, id_bien, id_source)
            )
            count += 1

            if count % 100 == 0:
                conn.commit()
                print(f"   ... {count} lignes insérées")

        except Exception as e:
            skipped += 1
            continue

    conn.commit()
    cursor.close()
    print(f" {count} annonces insérées | {skipped} ignorées")



def insert_valeurs_venales(conn):
    """Insère les valeurs vénales officielles dans MySQL
    Lit directement le fichier Excel source (pas besoin de Spark)
    """
    # Cherche le fichier Excel dans data/raw/sources/
    sources_dir = os.path.join(BASE_DIR, "data", "raw", "sources")
    excel_path = os.path.join(sources_dir, "valeurs_venales_togo.xlsx")

    if not os.path.exists(excel_path):
        print(f"     Fichier introuvable : {excel_path}")
        print("    Place valeurs_venales_togo.xlsx dans data/raw/sources/")
        return

    df = pd.read_excel(excel_path, engine="openpyxl")

    # Renommage des colonnes
    df = df.rename(columns={
        "Préfecture": "prefecture",
        "Zone": "zone_admin",
        "Quartier": "zone",
        "Valeur vénale (FCFA)": "prix",
        "Surface (m²)": "surface_m2",
        "Valeur/m² (FCFA)": "prix_m2_officiel",
    })

    # Normalisation zone
    df["zone"] = df["zone"].str.lower().str.strip()
    print(f"   {len(df)} valeurs vénales chargées depuis Excel")

    # Charger les zones en mémoire
    cursor = conn.cursor()
    cursor.execute("SELECT id, nom FROM zone_geographique")
    zones_map = {nom.lower().strip(): id_ for id_, nom in cursor.fetchall()}
    cursor.close()

    cursor = conn.cursor()
    count = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            zone_key = str(row.get("zone", "")).lower().strip()
            id_zone = zones_map.get(zone_key)

            if not id_zone:
                # Zone pas encore dans la table — on l'insère
                cursor.execute(
                    "INSERT IGNORE INTO zone_geographique (nom, commune, prefecture) VALUES (%s, %s, %s)",
                    (zone_key, "Lomé", str(row.get("prefecture", "GOLFE")))
                )
                conn.commit()
                cursor.execute("SELECT id FROM zone_geographique WHERE nom = %s", (zone_key,))
                result = cursor.fetchone()
                id_zone = result[0] if result else None

            if not id_zone:
                skipped += 1
                continue

            prix_m2 = float(row["prix_m2_officiel"]) if pd.notna(row.get("prix_m2_officiel")) else None
            surface = float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None
            prix = float(row["prix"]) if pd.notna(row.get("prix")) else None

            cursor.execute(
                """INSERT INTO valeur_venale (id_zone, prix_m2_officiel, surface_m2, valeur_totale)
                   VALUES (%s, %s, %s, %s)""",
                (id_zone, prix_m2, surface, prix)
            )
            count += 1

            if count % 50 == 0:
                conn.commit()

        except Exception as e:
            skipped += 1
            continue

    conn.commit()
    cursor.close()
    print(f"    {count} valeurs vénales insérées | {skipped} ignorées")

def run():
    print("  Connexion à MySQL...")
    conn = get_connection()

    print(" Vérification du schéma...")
    create_schema(conn)

    print(" Chargement des données nettoyées...")
    annonces_files = glob.glob(os.path.join(CLEANED_DIR, "annonces", "part-*.csv"))
    if not annonces_files:
        print(f" Aucun fichier trouvé dans : {os.path.join(CLEANED_DIR, 'annonces')}")
        print(" Lance d'abord : spark-submit pipeline/cleaning.py")
        conn.close()
        return

    df = pd.concat([pd.read_csv(f) for f in annonces_files], ignore_index=True)
    print(f"    {len(df)} lignes chargées")

    # Diagnostic colonnes
    print(f"    Colonnes disponibles : {df.columns.tolist()}")
    print(f"    Valeurs 'source' : {df['source'].dropna().unique().tolist()}")
    print(f"    Exemples 'zone'  : {df['zone'].dropna().unique()[:5].tolist()}")

    print("\n Insertion des sources...")
    insert_sources(conn, df)

    print(" Insertion des zones...")
    insert_zones(conn, df)

    print(" Insertion des biens et annonces...")
    insert_annonces(conn, df)

    print(" Insertion des valeurs vénales officielles...")
    insert_valeurs_venales(conn)

    conn.close()
    print("\n Modélisation terminée !")


if __name__ == "__main__":
    run()