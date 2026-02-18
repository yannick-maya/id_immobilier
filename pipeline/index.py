"""
Phase 5 — Calcul de l'indice ID Immobilier
Indice = (prix_moyen_m2_periode_N / prix_moyen_m2_reference) * 100
Base 100 = période de référence (la plus ancienne dans les données)
"""

import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "id_immobilier"),
}

PERIODE_REFERENCE = "GLOBAL"  # À remplacer par une vraie période quand tu auras des données temporelles


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def calculer_indice(conn):
    query = """
        SELECT id_zone, type_bien, type_offre, periode, prix_moyen_m2, nombre_annonces
        FROM statistiques_zone
        ORDER BY id_zone, type_bien, periode
    """
    df = pd.read_sql(query, conn)

    # Prix de référence par zone + type_bien
    reference = df[df["periode"] == PERIODE_REFERENCE].copy()
    reference = reference.rename(columns={"prix_moyen_m2": "prix_reference"})
    reference = reference[["id_zone", "type_bien", "prix_reference"]]

    # Fusion avec toutes les périodes
    df = df.merge(reference, on=["id_zone", "type_bien"], how="left")

    # Calcul de l'indice
    df["indice_valeur"] = (df["prix_moyen_m2"] / df["prix_reference"] * 100).round(4)

    # Tendance
    def tendance(indice):
        if indice > 105:
            return "HAUSSE"
        elif indice < 95:
            return "BAISSE"
        else:
            return "STABLE"

    df["tendance"] = df["indice_valeur"].apply(tendance)

    return df


def inserer_indice(conn, df):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        if pd.isna(row.get("indice_valeur")):
            continue
        cursor.execute(
            """INSERT INTO indice_immobilier
               (id_zone, type_bien, periode, prix_moyen_m2, indice_valeur, tendance)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                int(row["id_zone"]), str(row["type_bien"]),
                str(row["periode"]), float(row["prix_moyen_m2"]),
                float(row["indice_valeur"]), str(row["tendance"])
            )
        )
    conn.commit()
    cursor.close()
    print(f" {len(df)} indices insérés")


def afficher_tendances(df):
    print("\n RÉSUMÉ DES TENDANCES PAR ZONE :")
    print(df.groupby("tendance")["id_zone"].count().to_string())

    print("\n Zones en HAUSSE :")
    hausse = df[df["tendance"] == "HAUSSE"][["id_zone", "type_bien", "indice_valeur"]].head(5)
    print(hausse.to_string(index=False))

    print("\n Zones en BAISSE :")
    baisse = df[df["tendance"] == "BAISSE"][["id_zone", "type_bien", "indice_valeur"]].head(5)
    print(baisse.to_string(index=False))


def run():
    print(" Calcul de l'indice ID Immobilier...")
    conn = get_connection()
    df = calculer_indice(conn)
    inserer_indice(conn, df)
    afficher_tendances(df)
    conn.close()
    print(" Indice calculé !")


if __name__ == "__main__":
    run()
