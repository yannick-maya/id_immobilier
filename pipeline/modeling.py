"""
Phase 3 — Modélisation et stockage dans MySQL
Insertion des données nettoyées dans les tables relationnelles
Connexion via mysql-connector-python
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration MySQL ────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "id_immobilier"),
}

CLEANED_DIR = "data/cleaned/"


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def create_schema(conn):
    """Exécute le fichier schema.sql pour créer les tables"""
    cursor = conn.cursor()
    with open("sql/schema.sql", "r", encoding="utf-8") as f:
        sql = f.read()
    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt and not stmt.startswith("--"):
            cursor.execute(stmt)
    conn.commit()
    cursor.close()
    print(" Tables créées avec succès")


def insert_sources(conn):
    """Insère les sources de données"""
    sources = [
        ("ImmoAsk", "https://immoask.com", "2024-01-01", 500),
        ("Facebook", "https://facebook.com/marketplace", "2024-01-01", 80),
        ("CoinAfrique", "https://tg.coinafrique.com", "2024-01-01", 4844),
        ("ValeursVenales", "OTR/Cadastre Togo", "2024-01-01", 354),
    ]
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT IGNORE INTO source_donnees (nom, url, date_collecte, nombre_annonces) VALUES (%s, %s, %s, %s)",
        sources
    )
    conn.commit()
    cursor.close()
    print(f" {len(sources)} sources insérées")


def insert_zones(conn, df):
    """Insère les zones géographiques uniques"""
    zones = df["zone"].dropna().unique()
    cursor = conn.cursor()
    for zone in zones:
        cursor.execute(
            "INSERT IGNORE INTO zone_geographique (nom, commune, prefecture) VALUES (%s, %s, %s)",
            (zone, "Lomé", "GOLFE")
        )
    conn.commit()
    cursor.close()
    print(f" {len(zones)} zones insérées")


def get_zone_id(conn, zone_nom):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM zone_geographique WHERE nom = %s", (zone_nom,))
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None


def get_source_id(conn, source_nom):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM source_donnees WHERE nom = %s", (source_nom,))
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None


def insert_annonces(conn, df):
    """Insère les biens et annonces dans MySQL"""
    cursor = conn.cursor()
    count = 0

    for _, row in df.iterrows():
        try:
            id_zone = get_zone_id(conn, str(row.get("zone", "")).lower().strip())
            id_source = get_source_id(conn, str(row.get("source", "")))

            if not id_zone or not id_source:
                continue

            # Insérer bien
            cursor.execute(
                """INSERT INTO bien_immobilier (type_bien, type_offre, surface_m2, pieces, id_zone)
                   VALUES (%s, %s, %s, %s, %s)""",
                (
                    str(row.get("type_bien", "Inconnu")),
                    str(row.get("type_offre", "Inconnu")),
                    float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None,
                    int(row["pieces"]) if pd.notna(row.get("pieces")) else None,
                    id_zone
                )
            )
            id_bien = cursor.lastrowid

            # Calculer prix m²
            prix = float(row["prix"]) if pd.notna(row.get("prix")) else None
            surface = float(row["surface_m2"]) if pd.notna(row.get("surface_m2")) else None
            prix_m2 = round(prix / surface, 2) if prix and surface and surface > 0 else None

            # Insérer annonce
            cursor.execute(
                """INSERT INTO annonce (titre, prix, prix_m2, id_bien, id_source)
                   VALUES (%s, %s, %s, %s, %s)""",
                (str(row.get("titre", ""))[:255], prix, prix_m2, id_bien, id_source)
            )
            count += 1

        except Exception as e:
            print(f" Erreur ligne : {e}")
            continue

    conn.commit()
    cursor.close()
    print(f" {count} annonces insérées dans MySQL")


def run():
    print(" Connexion à MySQL...")
    conn = get_connection()

    print(" Création du schéma...")
    create_schema(conn)

    print(" Chargement des données nettoyées...")
    df = pd.read_csv(f"{CLEANED_DIR}annonces/part-*.csv")

    insert_sources(conn)
    insert_zones(conn, df)
    insert_annonces(conn, df)

    conn.close()
    print(" Modélisation terminée !")


if __name__ == "__main__":
    run()
