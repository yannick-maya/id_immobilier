"""
scraper_immoask.py
==================
Extrait les données BRUTES ImmoAsk via l'API GraphQL officielle
et les sauvegarde telles quelles dans data/raw/scraped/.



Usage :
    python scraper_immoask.py
    python scraper_immoask.py --limit 500
    python scraper_immoask.py --output data/raw/scraped/immoask_20240101.csv
"""

import requests
import pandas as pd
import argparse
import logging
from datetime import datetime
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────

API_URL = "https://devapi.omnisoft.africa/public/api/v2"
TIMEOUT = 30

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ─── SCRAPER ─────────────────────────────────────────────────────────────────

class ImmoAskScraper:
    """Récupère les données brutes de l'API ImmoAsk et les sauvegarde en CSV."""

    def __init__(self, limit: int = 500):
        self.limit    = limit
        self.raw_data = None
        self.df       = None

    def build_query(self) -> str:
        return (
            f"{{getPropertiesByKeyWords("
            f"orderBy:{{order:DESC,column:NUO}},"
            f"limit:{self.limit})"
            f"{{surface,id,nuo,usage,offre{{denomination}},"
            f"categorie_propriete{{denomination}},piece,titre,garage,"
            f"nuitee,cout_mensuel,ville{{denomination}},wc_douche_interne,"
            f"cout_vente,quartier{{id,denomination}}}}}}"
        )

    def fetch(self) -> dict:
        url = f"{API_URL}?query={self.build_query()}"
        log.info(f"Appel API ImmoAsk (limit={self.limit})…")
        resp = requests.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        self.raw_data = resp.json()
        count = len(self.raw_data["data"]["getPropertiesByKeyWords"])
        log.info(f"✅  {count} propriétés reçues")
        return self.raw_data

    def to_dataframe(self) -> pd.DataFrame:
        """Aplatit le JSON imbriqué → DataFrame brut. Aucune transformation."""
        if not self.raw_data:
            raise ValueError("Appelle fetch() d'abord.")

        rows = []
        for prop in self.raw_data["data"]["getPropertiesByKeyWords"]:
            rows.append({
                "source"      : "ImmoAsk",
                "id"          : prop.get("id"),
                "nuo"         : prop.get("nuo"),
                "titre"       : prop.get("titre"),
                "usage"       : prop.get("usage"),
                "offre"       : prop["offre"]["denomination"] if prop.get("offre") else None,
                "categorie"   : prop["categorie_propriete"]["denomination"] if prop.get("categorie_propriete") else None,
                "ville"       : prop["ville"]["denomination"] if prop.get("ville") else None,
                "quartier"    : prop["quartier"]["denomination"] if prop.get("quartier") else None,
                "piece"       : prop.get("piece"),
                "surface"     : prop.get("surface"),
                "garage"      : prop.get("garage"),
                "cout_mensuel": prop.get("cout_mensuel"),
                "cout_vente"  : prop.get("cout_vente"),
                "nuitee"      : prop.get("nuitee"),
            })

        self.df = pd.DataFrame(rows)
        log.info(f"DataFrame brut : {self.df.shape[0]} lignes × {self.df.shape[1]} colonnes")
        return self.df

    def save(self, path: str = None) -> str:
        if self.df is None:
            raise ValueError("Appelle to_dataframe() d'abord.")

        if path is None:
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = f"data/raw/scraped/immoask_{ts}.csv"

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(path, index=False, encoding="utf-8-sig")
        log.info(f"💾  Sauvegardé : {path} ({len(self.df)} lignes)")
        return path

    def run(self, output_path: str = None) -> pd.DataFrame:
        """fetch → to_dataframe → save."""
        self.fetch()
        self.to_dataframe()
        self.save(output_path)
        return self.df


# ─── POINT D'ENTRÉE ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper brut ImmoAsk → CSV raw")
    parser.add_argument("--limit",  type=int, default=500)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    scraper = ImmoAskScraper(limit=args.limit)
    df      = scraper.run(output_path=args.output)

    print(f"\n✅  {len(df)} propriétés sauvegardées")
    print(f"Colonnes brutes : {df.columns.tolist()}")
    print(df.head(3).to_string())


if __name__ == "__main__":
    main()