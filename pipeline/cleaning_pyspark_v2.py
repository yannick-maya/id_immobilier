"""
Phase 2 - Nettoyage V2 Spark
FIX : SparkSession creee dans run() uniquement, pas au niveau module.
      => Airflow peut charger ce fichier sans demarrer Spark.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
import os

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR     = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned_v2")
REJETS_DIR  = os.path.join(BASE_DIR, "data", "raw", "rejets")

# ── Regles metier ─────────────────────────────────────────────────────────────
ZONE_MAX_LEN = 30

PRIX_MIN = 10_000           # FCFA — en dessous : erreur de saisie
PRIX_MAX = 5_000_000_000    # FCFA — au dessus : aberrant (5 milliards)

MOTS_SUSPECTS = [
    "pharmacie", "cote de", "juste", "derriere", "face a",
    "avant", "apres", "carrefour", "forever", "standing",
    "meuble", "cuisine", "clinique", "goudron", "pave",
    "boulevard circulaire", "non loin", "a cote"
]

ZONES_INVALIDES = [
    "non spécifié", "non spécifiés", "togo", "nan", "", "none", "null"
]

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
# Utilisé pour les sources scrapées (immoask_scraped, etc.)
OFFRE_MAP = {
    "louer": "LOCATION", "location": "LOCATION",
    "vendre": "VENTE",   "vente": "VENTE",
    "bailler": "BAILLER","bail": "BAILLER",
}


# ── Helpers Spark ─────────────────────────────────────────────────────────────

def clean_prix(col_name):
    return F.regexp_replace(F.col(col_name).cast("string"), r"[^\d.]", "").cast(DoubleType())


def standardiser_type_bien(col_name):
    result = F.lower(F.trim(F.col(col_name)))
    for val_lower, val_standard in TYPES_BIEN_STANDARD.items():
        result = F.when(result == val_lower, F.lit(val_standard)).otherwise(result)
    return F.initcap(result)


def extraire_pieces_depuis_type(df):
    df = df.withColumn(
        "pieces_extrait",
        F.regexp_extract(F.lower(F.col("type_bien")), r"^(\d+)\s*chambre", 1).cast("int")
    )
    if "pieces" in df.columns:
        df = df.withColumn(
            "pieces",
            F.when(
                (F.col("pieces_extrait") > 0) & F.col("pieces").isNull(),
                F.col("pieces_extrait")
            ).otherwise(F.col("pieces"))
        )
    else:
        df = df.withColumn(
            "pieces",
            F.when(F.col("pieces_extrait") > 0, F.col("pieces_extrait")).otherwise(F.lit(None))
        )
    return df.drop("pieces_extrait")


def ajouter_raison_rejet(df):
    df = df.withColumn("raison_rejet", F.lit(None).cast(StringType()))

    df = df.withColumn("raison_rejet",
        F.when(F.length(F.col("zone")) > ZONE_MAX_LEN, F.lit("zone_trop_longue"))
         .otherwise(F.col("raison_rejet")))

    df = df.withColumn("raison_rejet",
        F.when(F.lower(F.col("zone")).isin(ZONES_INVALIDES) & F.col("raison_rejet").isNull(),
               F.lit("zone_invalide"))
         .otherwise(F.col("raison_rejet")))

    condition = F.lit(False)
    for mot in MOTS_SUSPECTS:
        condition = condition | F.lower(F.col("zone")).contains(mot)
    df = df.withColumn("raison_rejet",
        F.when(condition & F.col("raison_rejet").isNull(), F.lit("zone_description_lieu"))
         .otherwise(F.col("raison_rejet")))

    df = df.withColumn("raison_rejet",
        F.when(
            (F.col("prix").isNull() | F.col("surface_m2").isNull()) & F.col("raison_rejet").isNull(),
            F.lit("prix_ou_surface_manquant")
        ).otherwise(F.col("raison_rejet")))

    # Regle 5 : prix total aberrant
    df = df.withColumn("raison_rejet",
        F.when(
            (F.col("prix") < PRIX_MIN) & F.col("raison_rejet").isNull(),
            F.lit("prix_trop_bas")
        ).otherwise(F.col("raison_rejet")))

    df = df.withColumn("raison_rejet",
        F.when(
            (F.col("prix") > PRIX_MAX) & F.col("raison_rejet").isNull(),
            F.lit("prix_trop_eleve")
        ).otherwise(F.col("raison_rejet")))

    return df


# ── Normalisation sources scrapées ────────────────────────────────────────────

def normalise_scraped_immoask(df):
    """
    Convertit les colonnes brutes API ImmoAsk vers le format pipeline Spark.
    offre → type_offre (louer→LOCATION, vendre→VENTE, bailler→BAILLER)
    cout_mensuel / cout_vente → prix (selon type_offre)
    categorie → type_bien | quartier → zone | piece → pieces | surface → surface_m2
    """
    # Type offre
    offre_expr = F.lower(F.trim(F.col("offre")))
    type_offre = F.lit("NON SPECIFIE")
    for raw, standard in OFFRE_MAP.items():
        type_offre = F.when(offre_expr == raw, F.lit(standard)).otherwise(type_offre)
    df = df.withColumn("type_offre", type_offre)

    # Prix : VENTE → cout_vente, sinon → cout_mensuel
    df = df.withColumn("prix",
        F.when(F.col("type_offre") == "VENTE",
               F.col("cout_vente").cast(DoubleType()))
         .otherwise(F.col("cout_mensuel").cast(DoubleType()))
    )

    # Renommage
    rename_map = {
        "categorie": "type_bien",
        "quartier" : "zone",
        "piece"    : "pieces",
        "surface"  : "surface_m2",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Garder uniquement les colonnes pipeline
    cols = ["titre", "type_offre", "type_bien", "zone", "prix", "pieces", "surface_m2", "source"]
    df = df.select([c for c in cols if c in df.columns])

    return df


def normalise_scraped_coinafrique(df):
    """
    Convertit les colonnes brutes CoinAfrique vers le format pipeline Spark.
    Note : quartier et prix sont déjà propres (extraits dans le scraper).
    """
    # Type offre depuis colonne offre
    offre_expr = F.lower(F.trim(F.col("offre")))
    type_offre = F.lit("NON SPECIFIE")
    for raw, standard in [("louer","LOCATION"),("location","LOCATION"),("vendre","VENTE"),("vente","VENTE")]:
        type_offre = F.when(offre_expr == raw, F.lit(standard)).otherwise(type_offre)
    # Fallback depuis le titre
    titre_lower = F.lower(F.col("titre"))
    type_offre = F.when(type_offre == "NON SPECIFIE",
        F.when(titre_lower.contains("vente") | titre_lower.contains("vendre"), F.lit("VENTE"))
         .when(titre_lower.contains("location") | titre_lower.contains("louer"), F.lit("LOCATION"))
         .otherwise(F.lit("NON SPECIFIE"))
    ).otherwise(type_offre)
    df = df.withColumn("type_offre", type_offre)

    # Prix : déjà un entier depuis le scraper, juste caster
    df = df.withColumn("prix", F.col("prix").cast(DoubleType()))

    # Quartier : déjà propre depuis le scraper
    df = df.withColumn("quartier", F.coalesce(F.col("quartier"), F.lit("Non spécifié")))

    # Renommage
    rename_map = {
        "type_bien": "type_bien",
        "quartier" : "zone",
        "piece"    : "pieces",
        "surface"  : "surface_m2",
    }
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    cols = ["titre", "type_offre", "type_bien", "zone", "prix", "pieces", "surface_m2", "source"]
    df = df.select([c for c in cols if c in df.columns])
    return df


# Registre : ajouter ici les futures sources scrapées
SCRAPED_NORMALIZERS = {
    "immoask_scraped"    : normalise_scraped_immoask,
    "coinafrique_scraped": normalise_scraped_coinafrique,
}


# ── Cleaning principal ────────────────────────────────────────────────────────

def clean_annonces_v2(spark, source_name):
    path = os.path.join(RAW_DIR, f"{source_name}.csv")
    if not os.path.exists(path):
        print(f"  [!] Fichier non trouve : {path}")
        return None, None

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.withColumn("source", F.lit(source_name))

    # Normalisation spécifique si source scrapée
    if source_name in SCRAPED_NORMALIZERS:
        print(f"  [i] Normalisation colonnes brutes API : {source_name}")
        df = SCRAPED_NORMALIZERS[source_name](df)

    # Renommage standard (sources Excel)
    rename_map = {
        "Titre"       : "titre",
        "Type d'offre": "type_offre",
        "Type de bien": "type_bien",
        "Quartier"    : "zone",
        "Prix"        : "prix",
        "Piece"       : "pieces",
        "Surface"     : "surface_m2",
        "Source"      : "source_drop",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    if "source_drop" in df.columns:
        df = df.drop("source_drop")

    # Nettoyage de base
    df = df.withColumn("prix",       clean_prix("prix"))
    df = df.withColumn("surface_m2", F.col("surface_m2").cast(DoubleType()))
    df = df.withColumn("zone",       F.lower(F.trim(F.col("zone"))))

    if "type_bien" in df.columns:
        df = extraire_pieces_depuis_type(df)
        df = df.withColumn("type_bien", standardiser_type_bien("type_bien"))
    if "type_offre" in df.columns:
        df = df.withColumn("type_offre", F.upper(F.trim(F.col("type_offre"))))

    # Assurer la propagation des champs temporels (TODO 1.3)
    if "date_annonce" in df.columns:
        df = df.withColumn("date_annonce", F.to_date(F.col("date_annonce"), "yyyy-MM-dd"))
    else:
        df = df.withColumn("date_annonce", F.lit(None).cast(StringType()))

    if "annee" not in df.columns:
        df = df.withColumn("annee", F.year(F.col("date_annonce")))
    else:
        df = df.withColumn("annee", F.coalesce(F.col("annee"), F.year(F.col("date_annonce"))))

    if "trimestre" not in df.columns:
        df = df.withColumn("trimestre", F.quarter(F.col("date_annonce")))
    else:
        df = df.withColumn("trimestre", F.coalesce(F.col("trimestre"), F.quarter(F.col("date_annonce"))))

    if "periode" not in df.columns:
        df = df.withColumn("periode", F.concat(F.col("annee").cast(StringType()), F.lit("-Q"), F.col("trimestre").cast(StringType())))
    else:
        df = df.withColumn("periode", F.when(F.col("periode").isNull(), F.concat(F.col("annee").cast(StringType()), F.lit("-Q"), F.col("trimestre").cast(StringType()))).otherwise(F.col("periode")))

    # Règles de rejet
    df = ajouter_raison_rejet(df)

    df_valides = df.filter(F.col("raison_rejet").isNull()).drop("raison_rejet")
    df_rejetes = df.filter(F.col("raison_rejet").isNotNull())
    df_valides = df_valides.dropDuplicates(["titre", "prix", "zone", "surface_m2"])

    return df_valides, df_rejetes


def clean_venales_v2(spark):
    path = os.path.join(RAW_DIR, "valeursvenales.csv")
    if not os.path.exists(path):
        print("  [!] valeursvenales.csv non trouve")
        return None

    df = spark.read.csv(path, header=True, inferSchema=True)
    rename_map = {
        "Préfecture"          : "prefecture",
        "Zone"                : "zone_admin",
        "Quartier"            : "zone",
        "Valeur vénale (FCFA)": "prix",
        "Surface (m²)"        : "surface_m2",
        "Valeur/m² (FCFA)"    : "prix_m2_officiel",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    df = df.withColumn("prix",             clean_prix("prix"))
    df = df.withColumn("surface_m2",       F.col("surface_m2").cast(DoubleType()))
    df = df.withColumn("prix_m2_officiel", clean_prix("prix_m2_officiel"))
    df = df.withColumn("zone",             F.lower(F.trim(F.col("zone"))))
    return df.filter(F.col("zone").isNotNull() & F.col("prix_m2_officiel").isNotNull())


# ── Point d'entrée ────────────────────────────────────────────────────────────

def run():
    os.makedirs(CLEANED_DIR, exist_ok=True)
    os.makedirs(REJETS_DIR,  exist_ok=True)

    # ✅ SparkSession créée ICI, pas au niveau module
    #    => Airflow charge le fichier sans démarrer Spark
    spark = SparkSession.builder \
        .appName("IDImmobilier-Cleaning-V2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    sources = ["immoask", "facebook", "coinafrique", "immoask_scraped", "coinafrique_scraped"]
    dfs_valides = []
    dfs_rejetes = []
    total_avant = total_apres = total_rejetes = 0

    print("=" * 55)
    print("  CLEANING V2 SPARK - ID Immobilier")
    print("=" * 55)

    for s in sources:
        print(f"\n[{s.upper()}]")
        df_v, df_r = clean_annonces_v2(spark, s)
        if df_v is None:
            continue
        nb_v = df_v.count()
        nb_r = df_r.count()
        total_avant   += nb_v + nb_r
        total_apres   += nb_v
        total_rejetes += nb_r
        print(f"  Valides  : {nb_v}")
        print(f"  Rejetes  : {nb_r}")
        if nb_r > 0:
            df_r.groupBy("raison_rejet").count().orderBy("count", ascending=False).show(truncate=False)
        dfs_valides.append(df_v)
        dfs_rejetes.append(df_r)

    # Union et sauvegarde
    df_all_valides = dfs_valides[0]
    for df in dfs_valides[1:]:
        df_all_valides = df_all_valides.unionByName(df, allowMissingColumns=True)

    df_all_rejetes = dfs_rejetes[0]
    for df in dfs_rejetes[1:]:
        df_all_rejetes = df_all_rejetes.unionByName(df, allowMissingColumns=True)

    # S'assurer que les colonnes temporelles restent présentes en sortie
    for required_col in ["annee", "trimestre", "periode"]:
        if required_col not in df_all_valides.columns:
            df_all_valides = df_all_valides.withColumn(required_col, F.lit(None).cast(StringType()))

    out_annonces = os.path.join(CLEANED_DIR, "annonces")
    df_all_valides.write.mode("overwrite").csv(out_annonces, header=True)
    df_all_valides.write.mode("overwrite").parquet(out_annonces + "_parquet")

    out_rejets = os.path.join(REJETS_DIR, "annonces_rejetees")
    df_all_rejetes.write.mode("overwrite").csv(out_rejets, header=True)

    df_venales = clean_venales_v2(spark)
    if df_venales is not None:
        out_venales = os.path.join(CLEANED_DIR, "venales")
        df_venales.write.mode("overwrite").csv(out_venales, header=True)

    taux = round(total_rejetes / total_avant * 100, 2) if total_avant > 0 else 0
    print(f"\n{'='*55}")
    print(f"  Total valides  : {total_apres}")
    print(f"  Total rejetes  : {total_rejetes}")
    print(f"  Taux rejet     : {taux}%")
    print(f"{'='*55}")

    spark.stop()


if __name__ == "__main__":
    run()