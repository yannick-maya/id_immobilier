# 🏠 ID Immobilier — Indice Intelligent du Marché Immobilier au Togo

> Projet Big Data | Collecte, nettoyage, analyse et visualisation des prix immobiliers au Togo

---

## 📌 Contexte

Au Togo, le marché immobilier manque de transparence : les prix sont estimés informellement,
les annonces dispersées sur plusieurs plateformes, et les données officielles peu exploitées.
**ID Immobilier** construit un pipeline de données qui agrège plusieurs sources pour produire
un indice fiable du prix au m² par zone géographique.

---

## 🗂️ Sources de données

| Source | Type | Lignes | Description |
|--------|------|--------|-------------|
| ImmoAsk | Annonces web | 500 | Plateforme immobilière togolaise |
| Facebook Marketplace | Annonces réseaux sociaux | 80 | Annonces scrappées |
| CoinAfrique | Annonces web | 4 844 | Plateforme panafricaine |
| Valeurs Vénales OTR | Données officielles | 354 | Prix cadastraux officiels Togo |

---

## 🏗️ Architecture Big Data

```
┌─────────────────────────────────────────────────────────┐
│                    SOURCES DE DONNÉES                    │
│  ImmoAsk │ Facebook │ CoinAfrique │ Valeurs Vénales OTR  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              APACHE AIRFLOW (Orchestration)              │
│         DAG hebdomadaire — dag_immobilier.py             │
└──────────────────────┬──────────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
  ┌──────────────┐  ┌──────────────────────┐
  │  data/raw/   │  │  APACHE SPARK        │
  │  (CSV bruts) │→ │  (PySpark Cleaning)  │
  └──────────────┘  └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   data/cleaned/      │
                    │   (CSV + Parquet)    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   MySQL Database     │
                    │   id_immobilier      │
                    │  ┌────────────────┐  │
                    │  │ source_donnees │  │
                    │  │ zone_geo       │  │
                    │  │ bien_immob     │  │
                    │  │ annonce        │  │
                    │  │ stats_zone     │  │
                    │  │ indice_immo    │  │
                    │  └────────────────┘  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  STREAMLIT Dashboard │
                    │  Prix au m² / Zone   │
                    │  Indice ID Immob.    │
                    │  Cartes Folium       │
                    └──────────────────────┘
```

---

## 📁 Structure du projet

```
id_immobilier/
├── data/
│   ├── raw/         ← Fichiers sources CSV/Excel originaux
│   ├── cleaned/     ← Données nettoyées (CSV + Parquet)
│   └── gold/        ← Données agrégées finales
├── notebooks/       ← Exploration Jupyter
│   └── exploration.ipynb
├── pipeline/
│   ├── ingestion.py    ← Lecture des sources Excel → CSV
│   ├── cleaning.py     ← Nettoyage PySpark
│   ├── modeling.py     ← Insertion dans MySQL
│   ├── indicators.py   ← Calcul prix m², stats par zone
│   └── index.py        ← Calcul indice immobilier
├── dags/
│   └── dag_immobilier.py  ← DAG Airflow hebdomadaire
├── dashboard/
│   └── app.py          ← Dashboard Streamlit
├── sql/
│   └── schema.sql      ← Schéma MySQL complet
├── .env.example        ← Variables d'environnement
├── requirements.txt    ← Dépendances Python
└── README.md
```

---

## 🚀 Installation et lancement

### 1. Cloner et installer

```bash
git clone https://github.com/ton-username/id_immobilier.git
cd id_immobilier
pip install -r requirements.txt
```

### 2. Configurer MySQL

```bash
cp .env.example .env
# Édite .env avec tes identifiants MySQL
```

```bash
mysql -u root -p < sql/schema.sql
```

### 3. Placer les fichiers sources

```bash
# Copie tes 4 fichiers Excel dans :
data/raw/sources/
```

### 4. Lancer le pipeline manuellement

```bash
python pipeline/ingestion.py
spark-submit --master local[*] pipeline/cleaning.py
python pipeline/modeling.py
python pipeline/indicators.py
python pipeline/index.py
```

### 5. Lancer le dashboard

```bash
streamlit run dashboard/app.py
```

### 6. (Optionnel) Lancer Airflow

```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
# Puis active le DAG "id_immobilier_pipeline" dans l'interface
```

---

## 📊 Indicateurs produits

- Prix moyen au m² par zone et type de bien
- Prix médian au m² par zone
- Écart entre prix de marché et valeurs vénales officielles
- Indice immobilier ID Immobilier (Base 100)
- Tendances : HAUSSE / STABLE / BAISSE par zone

---

## 👨‍💻 Technologies utilisées

| Couche | Technologie |
|--------|------------|
| Ingestion | Python, pandas, openpyxl |
| Nettoyage | Apache Spark (PySpark) |
| Orchestration | Apache Airflow |
| Stockage | MySQL, Parquet |
| Analyse | pandas, SQL |
| Visualisation | Streamlit, Plotly, Folium |

---

## 🎓 Projet académique

Cours : Introduction au Big Data  
Encadrant : [Nom du professeur]  
Données : ImmoAsk, Facebook Marketplace, CoinAfrique, OTR Togo
