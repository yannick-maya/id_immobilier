"""
Phase 2 — Nettoyage et standardisation avec PySpark
Colonnes standardisées depuis les 4 sources :
  - ImmoAsk/Facebook/CoinAfrique : Source, Titre, Type d'offre, Type de bien, Quartier, Prix, Piece, Surface
  - ValeursVenales : Préfecture, Zone, Quartier, Valeur vénale (FCFA), Surface (m²), Valeur/m² (FCFA)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import re

spark = SparkSession.builder \
    .appName("IDImmobilier-Cleaning") \
    .getOrCreate()

RAW_DIR = "data/raw/"
CLEANED_DIR = "data/cleaned/"

# ─── Mapping colonnes vers standard ────────────────────────────────────────────
MAPPING_ANNONCES = {
    "Source": "source",
    "Titre": "titre",
    "Type d'offre": "type_offre",
    "Type de bien": "type_bien",
    "Quartier": "zone",
    "Prix": "prix",
    "Piece": "pieces",
    "Surface": "surface_m2",
}

MAPPING_VENALES = {
    "Préfecture": "prefecture",
    "Zone": "zone_admin",
    "Quartier": "zone",
    "Valeur vénale (FCFA)": "prix",
    "Surface (m²)": "surface_m2",
    "Valeur/m² (FCFA)": "prix_m2_officiel",
}


def clean_prix(col_name):
    """Nettoie une colonne prix : supprime espaces, FCFA, XOF, virgules"""
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^\d.]", "").cast(DoubleType())


def clean_annonces(source_name):
    df = spark.read.csv(f"{RAW_DIR}{source_name}.csv", header=True, inferSchema=True)

    # Renommage
    for old, new in MAPPING_ANNONCES.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Nettoyage prix et surface
    df = df.withColumn("prix", clean_prix("prix"))
    df = df.withColumn("surface_m2", F.col("surface_m2").cast(DoubleType()))

    # Normalisation zone
    df = df.withColumn("zone", F.lower(F.trim(F.col("zone"))))

    # Suppression valeurs aberrantes
    df = df.filter((F.col("prix") > 0) & (F.col("surface_m2") > 0))
    df = df.filter(F.col("zone") != "non spécifié")

    # Suppression doublons
    df = df.dropDuplicates(["titre", "prix", "zone"])

    return df


def clean_venales():
    df = spark.read.csv(f"{RAW_DIR}valeursvenales.csv", header=True, inferSchema=True)

    for old, new in MAPPING_VENALES.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    df = df.withColumn("prix", clean_prix("prix"))
    df = df.withColumn("surface_m2", F.col("surface_m2").cast(DoubleType()))
    df = df.withColumn("zone", F.lower(F.trim(F.col("zone"))))
    df = df.withColumn("source", F.lit("ValeursVenales"))

    return df


def run():
    print("🧹 Nettoyage en cours...")

    sources = ["immoask", "facebook", "coinafrique"]
    dfs = [clean_annonces(s) for s in sources]
    df_venales = clean_venales()

    # Union des annonces
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    # Sauvegarde
    df_all.write.mode("overwrite").csv(f"{CLEANED_DIR}annonces", header=True)
    df_all.write.mode("overwrite").parquet(f"{CLEANED_DIR}annonces_parquet")

    df_venales.write.mode("overwrite").csv(f"{CLEANED_DIR}venales", header=True)
    df_venales.write.mode("overwrite").parquet(f"{CLEANED_DIR}venales_parquet")

    print(f" Annonces nettoyées : {df_all.count()} lignes")
    print(f" Valeurs vénales nettoyées : {df_venales.count()} lignes")
    print(f" Données sauvegardées dans {CLEANED_DIR}")

    spark.stop()


if __name__ == "__main__":
    run()
