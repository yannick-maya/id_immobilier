"""
Phase 2 — Nettoyage et standardisation avec PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")

spark = SparkSession.builder \
    .appName("IDImmobilier-Cleaning") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def clean_prix(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^\d.]", "").cast(DoubleType())


def clean_annonces(source_name):
    path = os.path.join(RAW_DIR, f"{source_name}.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)

    print(f"   Colonnes brutes {source_name} : {df.columns}")

    # Supprimer la colonne 'Source' originale (ex: "ImmoAsk", "Facebook Marketplace")
    # car on va créer notre propre colonne 'source' propre
    if "Source" in df.columns:
        df = df.drop("Source")

    # Renommage vers standard
    rename_map = {
        "Titre": "titre",
        "Type d'offre": "type_offre",
        "Type de bien": "type_bien",
        "Quartier": "zone",
        "Prix": "prix",
        "Piece": "pieces",
        "Surface": "surface_m2",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Ajouter la colonne source propre
    df = df.withColumn("source", F.lit(source_name))

    # Nettoyage prix et surface
    df = df.withColumn("prix", clean_prix("prix"))
    df = df.withColumn("surface_m2", F.col("surface_m2").cast(DoubleType()))

    # Normalisation zone
    df = df.withColumn("zone", F.lower(F.trim(F.col("zone"))))

    # Suppression valeurs invalides
    df = df.filter((F.col("prix") > 0) & (F.col("surface_m2") > 0))
    df = df.filter(~F.lower(F.col("zone")).isin("non spécifié", "non spécifiés", ""))

    # Suppression doublons
    df = df.dropDuplicates(["titre", "prix", "zone"])

    return df


def clean_venales():
    path = os.path.join(RAW_DIR, "valeursvenales.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)

    rename_map = {
        "Préfecture": "prefecture",
        "Zone": "zone_admin",
        "Quartier": "zone",
        "Valeur vénale (FCFA)": "prix",
        "Surface (m²)": "surface_m2",
        "Valeur/m² (FCFA)": "prix_m2_officiel",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    df = df.withColumn("prix", clean_prix("prix"))
    df = df.withColumn("surface_m2", F.col("surface_m2").cast(DoubleType()))
    df = df.withColumn("zone", F.lower(F.trim(F.col("zone"))))
    df = df.withColumn("source", F.lit("ValeursVenales"))

    return df


def run():
    print(" Nettoyage en cours...")
    os.makedirs(CLEANED_DIR, exist_ok=True)

    sources = ["immoask", "facebook", "coinafrique"]
    dfs = []
    for s in sources:
        print(f"\n Traitement : {s}")
        df = clean_annonces(s)
        dfs.append(df)
        print(f"    {df.count()} lignes après nettoyage")

    # Union des annonces
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    # Vérification colonnes finales
    print(f"\n Colonnes finales : {df_all.columns}")
    print(f" Valeurs 'source' : {[r['source'] for r in df_all.select('source').distinct().collect()]}")

    # Sauvegarde
    out_annonces = os.path.join(CLEANED_DIR, "annonces")
    out_venales = os.path.join(CLEANED_DIR, "venales")

    df_all.write.mode("overwrite").csv(out_annonces, header=True)
    df_all.write.mode("overwrite").parquet(out_annonces + "_parquet")

    df_venales = clean_venales()
    df_venales.write.mode("overwrite").csv(out_venales, header=True)

    print(f"\n Annonces nettoyées : {df_all.count()} lignes")
    print(f" Valeurs vénales : {df_venales.count()} lignes")
    print(f" Sauvegardé dans : {CLEANED_DIR}")

    spark.stop()


if __name__ == "__main__":
    run()