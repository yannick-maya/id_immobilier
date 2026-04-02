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

Ce pipeline s’appuie sur Apache Spark pour le nettoyage (meilleure performance sur gros fichiers) et sur MongoDB (stockage NoSQL) :

```bash
# 4.1 Ingestion des sources brutes
python pipeline/ingestion.py

# 4.2 Nettoyage Spark (version 2.x ou nouvelles) - génère cleaned_v2
spark-submit --master local[*] pipeline/cleaning_v2_pandas.py
# ou
spark-submit --master local[*] pipeline/cleaning_pyspark_v2.py

# 4.3 Modélisation / insertion en MongoDB
# (depuis le csv nettoyé produit dans data/cleaned_v2/annonces_clean.csv)
python pipeline/modeling_mongodb.py

# 4.4 Calcul des indicateurs / indices (optionnel backtest MySQL existant)
python pipeline/indicators.py
python pipeline/index.py
```

> POSE : si tu veux passer par MySQL comme avant, tu peux également exécuter `python pipeline/modeling.py`.

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


  todo


  
# 1. Aller a la racine du projet
cd C:\Users\yanni\Desktop\Projet_Immobilier\id_immobilier

# 2. Lancer tous les conteneurs
docker-compose up -d

# 3. Verifier que tout tourne
docker-compose ps

# 4. Voir les logs si probleme
docker-compose logs -f airflow
docker-compose logs -f streamlit
```

---

## URLs apres lancement
```
Dashboard Streamlit  : http://localhost:8501
Airflow              : http://localhost:8081  (admin / admin1234)
Spark UI             : http://localhost:8080
MySQL                : localhost:3307
```

---

## Point important — XAMPP, MySQL et MongoDB

Comme tu utilises XAMPP sur le port 3306, le MySQL Docker est configure sur le **port 3307** pour eviter le conflit. Quand tu utilises Docker, change ton `.env` :
```
MYSQL_HOST=localhost
MYSQL_USER=immo_user
MYSQL_PASSWORD=immo1234
MYSQL_DB=id_immobilier
# Port 3307 pour Docker, 3306 pour XAMPP

# MongoDB (Atlas ou local) :
MONGO_URI=mongodb://localhost:27017
MONGO_DB=id_immobilier

# SPARK code

docker exec -it id_immobilier_spark /opt/spark/bin/spark-submit --master spark://spark:7077 /app/pipeline/cleaning_v2.py


PS C:\Users\yanni\Desktop\Projet_Immobilier\id_immobilier> docker exec -it id_immobilier_spark /opt/spark/bin/spark-submit --master spark://spark:7077 /app/pipeline/cleaning_v2.py
26/02/19 17:11:35 INFO SparkContext: Running Spark version 3.5.1
26/02/19 17:11:35 INFO SparkContext: OS info Linux, 6.6.87.2-microsoft-standard-WSL2, amd64
26/02/19 17:11:35 INFO SparkContext: Java version 11.0.22
Nettoyage V2 en cours...

Traitement : immoask
  Avant nettoyage : 460 lignes
  Apres nettoyage : 388 lignes valides
  Rejetes         : 72 lignes
  Repartition des rejets :
+------------------+-----+
|raison_rejet      |count|
+------------------+-----+
|prix_m2_trop_bas  |68   |
|prix_m2_trop_eleve|3    |
|zone_invalide     |1    |
+------------------+-----+


Traitement : facebook
  Avant nettoyage : 89 lignes
  Apres nettoyage : 31 lignes valides
  Rejetes         : 58 lignes
  Repartition des rejets :
+------------------------+-----+
|raison_rejet            |count|
+------------------------+-----+
|zone_invalide           |41   |
|prix_ou_surface_manquant|13   |
|prix_m2_trop_bas        |4    |
+------------------------+-----+


Traitement : coinafrique
  Avant nettoyage : 4841 lignes
  Apres nettoyage : 3572 lignes valides
  Rejetes         : 1269 lignes
  Repartition des rejets :
+------------------------+-----+
|raison_rejet            |count|
+------------------------+-----+
|prix_ou_surface_manquant|975  |
|prix_m2_trop_bas        |106  |
|zone_invalide           |70   |
|zone_description_lieu   |66   |
|zone_trop_longue        |35   |
|prix_m2_trop_eleve      |17   |
==================================================
Total avant nettoyage : 5390
Total valides         : 3991
Total rejetes         : 1399
Taux de rejet         : 25.96%
Valeurs venales       : 354

Valides  -> /app/data/cleaned_v2/annonces
Rejetes  -> /app/data/raw/rejets/annonces_rejetees
==================================================
PS C:\Users\yanni\Desktop\Projet_Immobilier\id_immobilier>










# 1. Lire les fichiers Excel et convertir en CSV
python pipeline/ingestion.py

# 2. Nettoyer avec Spark (filtrage + standardisation)
python pipeline/cleaning_v2.py

# 3. Insérer dans MySQL
python pipeline/modeling_v2.py

# 4. Calculer les statistiques par zone
python pipeline/indicators.py

# 5. Calculer l'indice immobilier
python pipeline/index.py

# 6. Lancer le dashboard
streamlit run dashboard/app.py
```

---

## Pourquoi cet ordre ?
```
ingestion     → produit les CSV dans data/raw/
cleaning_v2   → lit data/raw/  → produit data/cleaned_v2/
modeling_v2   → lit data/cleaned_v2/ → remplit MySQL       
      <!-- Avant (données brutes)  : 3 993 annonces | 28% rejet
      Apres (données camarade): 4 540 annonces |  5% rejet -->
indicators    → lit MySQL (table annonce) → remplit statistiques_zone
index         → lit statistiques_zone → remplit indice_immobilier
streamlit     → lit tout MySQL → affiche le dashboard

# airflow

docker exec -it id_immobilier_airflow airflow users create --username admin --password admin1234 --firstname Admin --lastname User --role Admin --email admin@idimmobilier.tg