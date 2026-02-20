"""
Phase 2 - Nettoyage v2 (version amelioree)
Differences avec cleaning.py v1 :
  - Regles de filtrage strictes sur les zones
  - Filtrage des prix au m2 aberrants (< 1000 ou > 2 000 000 FCFA)
  - Sauvegarde des donnees rejetees dans data/raw/rejets/
  - Traçabilite complete des raisons de rejet
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned_v2")
REJETS_DIR  = os.path.join(BASE_DIR, "data", "raw", "rejets")

spark = SparkSession.builder \
    .appName("IDImmobilier-Cleaning-V2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Regles metier ─────────────────────────────────────────────────────────────
PRIX_M2_MIN  = 1_000       # FCFA/m2 minimum acceptable
PRIX_M2_MAX  = 2_000_000   # FCFA/m2 maximum acceptable
ZONE_MAX_LEN = 30          # Longueur max d un nom de zone

MOTS_SUSPECTS = [
    "pharmacie", "cote de", "juste", "derriere", "face a",
    "avant", "apres", "carrefour", "forever", "standing",
    "meuble", "cuisine", "clinique", "goudron", "pave",
    "boulevard circulaire", "non loin", "a cote", "non loin"
]

ZONES_INVALIDES = [
    "non spécifié", "non spécifiés", "togo", "nan", "", "none", "null"
]

# ── Standardisation des types de biens ───────────────────────────────────────
# Logique :
# - "2 chambres", "3 chambres" = Maison avec N pieces (chiffre -> pieces)
# - "Chambre" seul = chambre meublee/studio a louer
# - La difference entre biens se fait via la colonne "pieces"

TYPES_BIEN_STANDARD = {
    # Chambre seule (location meublee, cite universitaire)
    "chambre": "Chambre",
    "chambre meublee": "Chambre",
    "chambre non meublee": "Chambre",
    "studio": "Studio",

    # Maison avec N chambres -> Maison (le chiffre ira dans pieces)
    "1 chambre": "Maison",
    "2 chambres": "Maison",
    "3 chambres": "Maison",
    "4 chambres": "Maison",
    "5 chambres": "Maison",
    "6 chambres": "Maison",
    "1chambre": "Maison",
    "2chambre": "Maison",
    "3chambre": "Maison",
    "4chambre": "Maison",
    "5chambre": "Maison",
    "6chambre": "Maison",
    "1 chambre a coucher": "Maison",
    "2 chambres a coucher": "Maison",
    "3 chambres a coucher": "Maison",
    "4 chambres a coucher": "Maison",
    "maison": "Maison",
    "maisons": "Maison",

    # Villa
    "villa": "Villa",
    "villas": "Villa",
    "villa moderne": "Villa",
    "villa duplex": "Villa",

    # Appartement
    "appartement": "Appartement",
    "appartements": "Appartement",
    "appart": "Appartement",
    "appartement meuble": "Appartement",

    # Terrain
    "terrain": "Terrain",
    "terrains": "Terrain",
    "terrain agricole": "Terrain Agricole",
    "terrains agricoles": "Terrain Agricole",
    "terrain a batir": "Terrain",

    # Bureau / Commerce
    "bureau": "Bureau",
    "bureaux": "Bureau",
    "boutique": "Boutique",
    "boutiques": "Boutique",
    "magasin": "Magasin",
    "commerce": "Commerce",
    "local commercial": "Commerce",

    # Immeuble
    "immeuble": "Immeuble",
    "immeubles": "Immeuble",
    "immeuble de rapport": "Immeuble",

    # Entrepot
    "entrepot": "Entrepot",
    "entrepôt": "Entrepot",

    # Chambre salon (chambre + salon = Maison, chiffre -> pieces)
    "chambre salon": "Chambre Salon",
    "1 chambre salon": "Maison",
    "2 chambres salon": "Maison",
    "3 chambres salon": "Maison",
    "4 chambres salon": "Maison",
    "1chambre salon": "Maison",
    "2chambre salon": "Maison",
    "3chambre salon": "Maison",

    # Bureau / Commerce variantes
    "bureau/commerce": "Commerce",
    "espace commercial": "Commerce",
    "local commercial": "Commerce",
    "immeuble commercial": "Immeuble",

    # Terrain variantes
    "terrain urbain": "Terrain",

    # Hotel / Bar / Ecole / Station
    "bar": "Bar",
    "hotel": "Hotel",
    "ecole": "Ecole",
    "station": "Station Service",

    # Versions meublees -> meme type
    "studio meuble": "Studio",
    "studio meublé": "Studio",
    "villa meublee": "Villa",
    "villa meublée": "Villa",
    "appartement meuble": "Appartement",
    "appartement meublé": "Appartement",
    "chambre meublee": "Chambre",
    "chambre meublée": "Chambre",
}

def standardiser_type_bien(col_name):
    """Standardise les types de biens via mapping."""
    result = F.lower(F.trim(F.col(col_name)))
    for val_lower, val_standard in TYPES_BIEN_STANDARD.items():
        result = F.when(result == val_lower, F.lit(val_standard)).otherwise(result)
    # Capitaliser la premiere lettre pour les cas non mappes
    result = F.initcap(result)
    return result

def extraire_pieces_depuis_type(df):
    """
    Extrait le nombre de pieces depuis le type de bien si present.
    Ex: "2 chambres"           -> type_bien="Maison", pieces=2
        "3chambre"             -> type_bien="Maison", pieces=3
        "3 chambres a coucher" -> type_bien="Maison", pieces=3
        "Chambre"              -> type_bien="Chambre", pieces=inchange
    """
    # Extraire le chiffre devant "chambre" (avec ou sans espace, avec ou sans "a coucher")
    df = df.withColumn(
        "pieces_extrait",
        F.regexp_extract(
            F.lower(F.col("type_bien")),
            r"^(\d+)\s*chambre",  # capture: "2 chambres", "3chambre", "4 chambres a coucher"
            1
        ).cast("int")
    )
    # Remplir pieces seulement si on a extrait un chiffre et que pieces est vide
    if "pieces" in df.columns:
        df = df.withColumn(
            "pieces",
            F.when(
                (F.col("pieces_extrait") > 0) & (F.col("pieces").isNull()),
                F.col("pieces_extrait")
            ).otherwise(F.col("pieces"))
        )
    else:
        df = df.withColumn(
            "pieces",
            F.when(F.col("pieces_extrait") > 0, F.col("pieces_extrait")).otherwise(F.lit(None))
        )
    return df.drop("pieces_extrait")

def standardiser_type_offre(col_name):
    """Standardise VENTE/LOCATION en majuscules."""
    return F.upper(F.trim(F.col(col_name)))



def clean_prix(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^\d.]", "").cast(DoubleType())


def ajouter_raison_rejet(df):
    df = df.withColumn("raison_rejet", F.lit(None).cast(StringType()))

    # Regle 1 : zone trop longue
    df = df.withColumn("raison_rejet",
        F.when(F.length(F.col("zone")) > ZONE_MAX_LEN, F.lit("zone_trop_longue"))
         .otherwise(F.col("raison_rejet"))
    )

    # Regle 2 : zone invalide connue
    df = df.withColumn("raison_rejet",
        F.when(
            F.lower(F.col("zone")).isin(ZONES_INVALIDES) & F.col("raison_rejet").isNull(),
            F.lit("zone_invalide")
        ).otherwise(F.col("raison_rejet"))
    )

    # Regle 3 : zone contient un mot suspect
    condition = F.lit(False)
    for mot in MOTS_SUSPECTS:
        condition = condition | F.lower(F.col("zone")).contains(mot)
    df = df.withColumn("raison_rejet",
        F.when(condition & F.col("raison_rejet").isNull(), F.lit("zone_description_lieu"))
         .otherwise(F.col("raison_rejet"))
    )

    # Regle 4 : prix ou surface manquant
    df = df.withColumn("raison_rejet",
        F.when(
            (F.col("prix").isNull() | F.col("surface_m2").isNull()) & F.col("raison_rejet").isNull(),
            F.lit("prix_ou_surface_manquant")
        ).otherwise(F.col("raison_rejet"))
    )

    # Regle 5 : calcul prix m2 et filtrage aberrations
    df = df.withColumn("prix_m2_calc",
        F.when(
            F.col("surface_m2") > 0,
            F.col("prix") / F.col("surface_m2")
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    df = df.withColumn("raison_rejet",
        F.when(
            (F.col("prix_m2_calc") > PRIX_M2_MAX) & F.col("raison_rejet").isNull(),
            F.lit("prix_m2_trop_eleve")
        ).otherwise(F.col("raison_rejet"))
    )
    df = df.withColumn("raison_rejet",
        F.when(
            (F.col("prix_m2_calc") < PRIX_M2_MIN) & F.col("raison_rejet").isNull(),
            F.lit("prix_m2_trop_bas")
        ).otherwise(F.col("raison_rejet"))
    )

    return df.drop("prix_m2_calc")


def clean_annonces_v2(source_name):
    path = os.path.join(RAW_DIR, f"{source_name}.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Supprimer colonne Source originale
    if "Source" in df.columns:
        df = df.drop("Source")

    # Renommage standard
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

    df = df.withColumn("source", F.lit(source_name))
    df = df.withColumn("prix", clean_prix("prix"))
    df = df.withColumn("surface_m2", F.col("surface_m2").cast(DoubleType()))
    df = df.withColumn("zone", F.lower(F.trim(F.col("zone"))))

    # Standardisation des types
    if "type_bien" in df.columns:
        # 1. Extraire le nb de pieces AVANT la standardisation (pendant que "2 chambres" est encore lisible)
        df = extraire_pieces_depuis_type(df)
        # 2. Puis standardiser le type
        df = df.withColumn("type_bien", standardiser_type_bien("type_bien"))
    if "type_offre" in df.columns:
        df = df.withColumn("type_offre", standardiser_type_offre("type_offre"))

    # Appliquer les regles de rejet
    df = ajouter_raison_rejet(df)

    # Separer valides et rejetes
    df_valides = df.filter(F.col("raison_rejet").isNull()).drop("raison_rejet")
    df_rejetes = df.filter(F.col("raison_rejet").isNotNull())

    # Deduplication stricte sur les valides
    df_valides = df_valides.dropDuplicates(["titre", "prix", "zone", "surface_m2"])

    return df_valides, df_rejetes


def clean_venales_v2():
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
    print("Nettoyage V2 en cours...")
    os.makedirs(CLEANED_DIR, exist_ok=True)
    os.makedirs(REJETS_DIR, exist_ok=True)

    sources = ["immoask", "facebook", "coinafrique"]
    dfs_valides = []
    dfs_rejetes = []
    total_avant = 0
    total_apres = 0
    total_rejetes = 0

    for s in sources:
        print(f"\nTraitement : {s}")
        df_v, df_r = clean_annonces_v2(s)
        nb_v = df_v.count()
        nb_r = df_r.count()
        total_avant  += nb_v + nb_r
        total_apres  += nb_v
        total_rejetes += nb_r

        print(f"  Avant nettoyage : {nb_v + nb_r} lignes")
        print(f"  Apres nettoyage : {nb_v} lignes valides")
        print(f"  Rejetes         : {nb_r} lignes")
        print("  Repartition des rejets :")
        df_r.groupBy("raison_rejet").count().orderBy("count", ascending=False).show(truncate=False)

        dfs_valides.append(df_v)
        dfs_rejetes.append(df_r)

    # Union valides
    df_all_valides = dfs_valides[0]
    for df in dfs_valides[1:]:
        df_all_valides = df_all_valides.unionByName(df, allowMissingColumns=True)

    # Union rejetes
    df_all_rejetes = dfs_rejetes[0]
    for df in dfs_rejetes[1:]:
        df_all_rejetes = df_all_rejetes.unionByName(df, allowMissingColumns=True)

    # Sauvegarde valides dans cleaned_v2/
    out_annonces = os.path.join(CLEANED_DIR, "annonces")
    df_all_valides.write.mode("overwrite").csv(out_annonces, header=True)
    df_all_valides.write.mode("overwrite").parquet(out_annonces + "_parquet")

    # Sauvegarde rejetes
    out_rejets = os.path.join(REJETS_DIR, "annonces_rejetees")
    df_all_rejetes.write.mode("overwrite").csv(out_rejets, header=True)

    # Valeurs venales
    df_venales = clean_venales_v2()
    out_venales = os.path.join(CLEANED_DIR, "venales")
    df_venales.write.mode("overwrite").csv(out_venales, header=True)

    # Rapport de nettoyage
    taux_rejet = round(total_rejetes / total_avant * 100, 2) if total_avant > 0 else 0
    print(f"\n{'='*50}")
    print(f"RAPPORT DE NETTOYAGE V2")
    print(f"{'='*50}")
    print(f"Total avant nettoyage : {total_avant}")
    print(f"Total valides         : {total_apres}")
    print(f"Total rejetes         : {total_rejetes}")
    print(f"Taux de rejet         : {taux_rejet}%")
    print(f"Valeurs venales       : {df_venales.count()}")
    print(f"\nValides  -> {out_annonces}")
    print(f"Rejetes  -> {out_rejets}")
    print(f"{'='*50}")

    spark.stop()


if __name__ == "__main__":
    run()