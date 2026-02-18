"""
Phase 4 — Calcul des indicateurs statistiques
Prix au m², moyenne/médiane par zone, écart vs valeurs vénales officielles
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


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def calculer_statistiques(conn):
    """Calcule les stats par zone et type de bien"""

    query = """
        SELECT
            z.id AS id_zone,
            z.nom AS zone,
            b.type_bien,
            b.type_offre,
            a.prix_m2,
            a.prix
        FROM annonce a
        JOIN bien_immobilier b ON a.id_bien = b.id
        JOIN zone_geographique z ON b.id_zone = z.id
        WHERE a.prix_m2 IS NOT NULL AND a.prix_m2 > 0
    """

    df = pd.read_sql(query, conn)

    # Calcul des statistiques groupées
    stats = df.groupby(["id_zone", "zone", "type_bien", "type_offre"]).agg(
        prix_moyen_m2=("prix_m2", "mean"),
        prix_median_m2=("prix_m2", "median"),
        prix_min=("prix", "min"),
        prix_max=("prix", "max"),
        nombre_annonces=("prix_m2", "count")
    ).reset_index()

    stats["prix_moyen_m2"] = stats["prix_moyen_m2"].round(2)
    stats["prix_median_m2"] = stats["prix_median_m2"].round(2)
    stats["periode"] = "GLOBAL"

    # Calcul écart vs valeurs vénales
    venales_query = """
        SELECT z.nom AS zone, vv.prix_m2_officiel
        FROM valeur_venale vv
        JOIN zone_geographique z ON vv.id_zone = z.id
    """
    df_venales = pd.read_sql(venales_query, conn)
    stats = stats.merge(df_venales, on="zone", how="left")
    stats["ecart_valeur_venale"] = (
        (stats["prix_moyen_m2"] - stats["prix_m2_officiel"]) / stats["prix_m2_officiel"] * 100
    ).round(2)

    return stats


def inserer_statistiques(conn, stats):
    cursor = conn.cursor()
    for _, row in stats.iterrows():
        cursor.execute(
            """INSERT INTO statistiques_zone
               (id_zone, type_bien, type_offre, periode, prix_moyen_m2, prix_median_m2,
                prix_min, prix_max, nombre_annonces, ecart_valeur_venale)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                int(row["id_zone"]), str(row["type_bien"]), str(row["type_offre"]),
                str(row["periode"]), float(row["prix_moyen_m2"]), float(row["prix_median_m2"]),
                float(row["prix_min"]), float(row["prix_max"]), int(row["nombre_annonces"]),
                float(row["ecart_valeur_venale"]) if pd.notna(row.get("ecart_valeur_venale")) else None
            )
        )
    conn.commit()
    cursor.close()
    print(f" {len(stats)} statistiques insérées")


def afficher_top_zones(stats):
    print("\n TOP 5 zones les plus chères (prix moyen m²) :")
    top = stats.sort_values("prix_moyen_m2", ascending=False).head(5)
    print(top[["zone", "type_bien", "prix_moyen_m2", "nombre_annonces"]].to_string(index=False))

    print("\n TOP 5 zones les moins chères :")
    bottom = stats.sort_values("prix_moyen_m2").head(5)
    print(bottom[["zone", "type_bien", "prix_moyen_m2", "nombre_annonces"]].to_string(index=False))


def run():
    print(" Calcul des indicateurs...")
    conn = get_connection()
    stats = calculer_statistiques(conn)
    inserer_statistiques(conn, stats)
    afficher_top_zones(stats)
    conn.close()
    print(" Indicateurs calculés !")


if __name__ == "__main__":
    run()
