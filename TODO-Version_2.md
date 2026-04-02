Voici la feuille de route complete.

---

# ID Immobilier — Feuille de Route Professionnelle
## Version 2.0 · Plateforme Data-Driven Immobiliere · Togo

---

## Vision du produit final


```
┌─────────────────────────────────────────────────────────┐
│                   ID IMMOBILIER v2.0                    │
│                                                         │
│  ┌─────────────────┐      ┌─────────────────────────┐  │
│  │  PLATEFORME     │      │  BACK-OFFICE ADMIN      │  │
│  │  UTILISATEUR    │      │                         │  │
│  │                 │      │  Gestion users          │  │
│  │  Inscription    │      │  Gestion annonces       │  │
│  │  Connexion      │      │  Monitoring pipeline    │  │
│  │  Recherche bien │      │  Tableaux de bord TDR   │  │
│  │  Indice marche  │      │  OKR / KPIs             │  │
│  │  Scoring IA     │      │  Gouvernance donnees    │  │
│  └────────┬────────┘      └───────────┬─────────────┘  │
│           │                           │                 │
│           └──────────┬────────────────┘                 │
│                      │                                  │
│              ┌───────▼────────┐                         │
│              │   API FastAPI  │                         │
│              │   + Auth JWT   │                         │
│              └───────┬────────┘                         │
│                      │                                  │
│              ┌───────▼────────┐                         │
│              │    MongoDB     │                         │
│              │  + Pipeline    │                         │
│              │  Spark/Airflow │                         │
│              └────────────────┘                         │
└─────────────────────────────────────────────────────────┘
```

---

## Pourquoi MongoDB et pas PostgreSQL

| Critere | PostgreSQL | MongoDB |
|---|---|---|
| Schema des annonces | Rigide — toutes les colonnes fixees | Flexible — chaque annonce peut avoir des champs differents |
| Requetes geospatiales | PostGIS (extension a installer) | Natif — `$near`, `$geoWithin` integres |
| Donnees heterogenes | Difficile — 4 sources avec structures differentes | Naturel — chaque document a son propre format |
| Agregations analytiques | SQL classique | Pipeline d'agregation puissant |
| Evolution du schema | Migration obligatoire | Ajout de champs sans impact |
| Hebergement gratuit | Render / Railway | MongoDB Atlas (512MB gratuit) |

---

## Stack technique final

```
COLLECTE          Python · BeautifulSoup · requests · GraphQL
TRAITEMENT        Apache Spark 3.5 · PySpark
ORCHESTRATION     Apache Airflow 2.x
STOCKAGE          MongoDB Atlas (cloud) + Motor (driver async)
BACKEND           FastAPI + Pydantic v2 + JWT (python-jose)
FRONTEND USER     React + Tailwind CSS + Leaflet (carte)
FRONTEND ADMIN    React + Recharts + shadcn/ui
DASHBOARD INTERNE Streamlit (analytics + demo TDR)
INFRA             Docker · Docker Compose · Render.com
CI/CD             GitHub Actions
```

---

## Les 5 Chantiers

```
Chantier 1 — Migration MongoDB + Schema
Chantier 2 — Variable periode + Indice temporel
Chantier 3 — API FastAPI complete
Chantier 4 — Plateforme utilisateur (inscription, recherche)
Chantier 5 — Back-office admin
```

---

## Chantier 1 — Migration MongoDB + Schema

**Objectif** : remplacer MySQL par MongoDB, adapter tout le pipeline.

**Fichiers modifies**

| Fichier | Changement |
|---|---|
| `docker-compose.yml` | Remplacer service `mysql` par `mongo` |
| `modeling_mongodb.py` | Nouveau — remplace `modeling_mysql_v2.py` |
| `indicators.py` | Connecteur PyMongo au lieu de SQLAlchemy |
| `index.py` | Idem |
| `dashboard.py` | `pymongo` au lieu de `mysql.connector` |
| `.env` | `MONGO_URI=mongodb+srv://...` |
| `requirements.txt` | Ajouter `pymongo motor beanie` |

**Structure des collections MongoDB**

```
id_immobilier (database)
├── annonces          ← document par annonce
├── zones             ← document par zone
├── statistiques      ← prix moyen/median par zone+type
├── indices           ← indice par zone+periode
├── valeurs_venales   ← reference OTR
├── sources           ← metadata des sources
└── users             ← comptes utilisateurs
```

**Document type `annonces`**
```json
{
  "_id": "ObjectId",
  "titre": "Villa 4 pieces a Tokoin",
  "prix": 45000000,
  "prix_m2": 187500,
  "surface_m2": 240,
  "type_bien": "Villa",
  "type_offre": "VENTE",
  "zone": "Tokoin",
  "source": "immoask",
  "periode": "2025-Q1",
  "annee": 2025,
  "trimestre": 1,
  "date_annonce": "2025-01-15",
  "localisation": {
    "type": "Point",
    "coordinates": [1.2123, 6.1375]
  },
  "created_at": "2025-01-15T08:00:00Z"
}
```

---

## Chantier 2 — Variable periode + Indice temporel

**Objectif** : calculer l'indice par periode (trimestre/annee) pour voir l'evolution dans le temps.

**Ce qu'on ajoute partout**

| Fichier | Ajout |
|---|---|
| `ingestion.py` | Extraction `annee`, `trimestre`, `periode` depuis `date_annonce` |
| `cleaning_pyspark_v2.py` | Propagation des colonnes periode dans le DataFrame |
| `index.py` | Calcul GROUP BY zone + periode au lieu de global |
| `dashboard.py` | Filtre sidebar "Periode" + graphique evolution temporelle |

**Format periode** : `"2025-Q1"`, `"2025-Q2"`, `"2024-Q4"` — un trimestre par document d'indice.

---

## Chantier 3 — API FastAPI

**Objectif** : exposer toutes les donnees via une API REST documentee, securisee, consommable par le frontend.

**Structure des fichiers**
```
api/
├── main.py
├── database.py          ← connexion MongoDB (Motor async)
├── auth/
│   ├── jwt.py           ← generation et validation tokens
│   ├── middleware.py    ← protection des routes
│   └── password.py      ← hash bcrypt
├── models/
│   ├── user.py          ← schema Pydantic User
│   ├── annonce.py       ← schema Pydantic Annonce
│   └── indice.py        ← schema Pydantic Indice
└── routers/
    ├── auth.py          ← /register /login /me
    ├── annonces.py      ← /annonces (CRUD + filtres)
    ├── zones.py         ← /zones
    ├── statistiques.py  ← /statistiques
    ├── indice.py        ← /indice (avec periode)
    ├── recherche.py     ← /recherche (full-text + geo)
    ├── favoris.py       ← /favoris (user seulement)
    └── admin.py         ← /admin/* (admin seulement)
```

**Tous les endpoints**

```
AUTH
POST   /auth/register          Inscription utilisateur
POST   /auth/login             Connexion → token JWT
GET    /auth/me                Profil utilisateur connecte
PUT    /auth/me                Modifier son profil
POST   /auth/logout            Deconnexion

ANNONCES
GET    /annonces               Liste avec filtres (zone, type, prix, periode)
GET    /annonces/{id}          Detail d'une annonce
POST   /annonces               Soumettre une annonce (user)
GET    /annonces/search        Recherche full-text

ZONES
GET    /zones                  Liste toutes les zones
GET    /zones/{nom}            Detail d'une zone + stats

STATISTIQUES
GET    /statistiques           Prix moyen/median par zone
GET    /statistiques/{zone}    Stats d'une zone (avec historique)

INDICE
GET    /indice                 Indice par zone et periode
GET    /indice/{zone}          Evolution historique d'une zone
GET    /indice/tendances        Zones HAUSSE / STABLE / BAISSE

COMPARAISON
GET    /comparaison/otr        Marche vs valeurs venales OTR

FAVORIS (user connecte)
GET    /favoris                Mes annonces sauvegardees
POST   /favoris/{id}           Ajouter aux favoris
DELETE /favoris/{id}           Retirer des favoris

ADMIN
GET    /admin/users            Liste des utilisateurs
PUT    /admin/users/{id}       Modifier un utilisateur
DELETE /admin/users/{id}       Supprimer un utilisateur
GET    /admin/pipeline         Etat du pipeline Airflow
GET    /admin/annonces         Toutes annonces (+ non validees)
PUT    /admin/annonces/{id}    Valider / refuser une annonce
GET    /admin/stats            Stats globales de la plateforme
GET    /admin/okr              Tableau de bord OKR
```

---

## Chantier 4 — Plateforme Utilisateur

**Objectif** : interface web pour le grand public — inscription, connexion, recherche, consultation.

**Pages**
```
/                    Page d'accueil (prix par zone, carte)
/register            Inscription
/login               Connexion
/dashboard           Tableau de bord personnel
/recherche           Recherche avancee de biens
/bien/{id}           Fiche detaillee d'un bien
/favoris             Mes biens sauvegardes
/indice              Carte de l'indice par zone
/simulateur          Simulateur de prix IA
/profil              Mon profil
```

**Fonctionnalites utilisateur**
- Inscription avec email + mot de passe
- Recherche par zone, type de bien, fourchette de prix, surface
- Carte interactive Leaflet avec les annonces geolocalises
- Fiche detaillee avec estimation IA du prix
- Systeme de favoris
- Comparaison de zones
- Alerte prix — notification si un bien sous le prix du marche apparait

---

## Chantier 5 — Back-Office Admin

**Objectif** : interface de gestion pour l'equipe ID Immobilier.

**Pages admin**
```
/admin                      Dashboard general
/admin/pipeline             Monitoring Airflow en temps reel
/admin/annonces             Validation des annonces soumises
/admin/users                Gestion des utilisateurs
/admin/indice               Suivi des indices par periode
/admin/okr                  Tableau de bord OKR (comme dans la demo TDR)
/admin/gouvernance          Matrice propriete des donnees
/admin/qualite              Taux de rejet par source
/admin/logs                 Logs du pipeline
```

---

## Planning par phase

```
PHASE 0 — Fondations (Semaine 1-2)
  · Migration MongoDB + schema des collections
  · Docker Compose mis a jour
  · Tests pipeline complet sur MongoDB
  · Variables d'environnement production

PHASE 1 — Backend API (Semaine 3-5)
  · FastAPI + connexion MongoDB Motor
  · Authentification JWT (register/login/me)
  · Endpoints annonces, zones, statistiques
  · Endpoints indice avec periode
  · Endpoints admin
  · Documentation Swagger automatique
  · Tests unitaires des endpoints

PHASE 2 — Variable periode (Semaine 4-5)
  · Modification ingestion.py
  · Modification index.py (calcul par trimestre)
  · Ajout filtre periode dans dashboard Streamlit
  · Graphique evolution historique dans Streamlit

PHASE 3 — Plateforme utilisateur (Semaine 6-9)
  · Setup React + Tailwind + React Router
  · Pages inscription et connexion
  · Page d'accueil avec carte Leaflet
  · Page recherche avec filtres
  · Fiche bien avec scoring IA
  · Systeme favoris
  · Alertes prix

PHASE 4 — Back-office admin (Semaine 8-10)
  · Interface admin React
  · Dashboard pipeline monitoring
  · Validation des annonces
  · Gestion utilisateurs
  · Tableaux de bord OKR integres (demo TDR)

PHASE 5 — Hebergement et production (Semaine 11-12)
  · MongoDB Atlas (cloud)
  · API FastAPI deployee sur Render
  · Frontend deploye sur Vercel ou Netlify
  · Variables d'environnement production
  · GitHub Actions CI/CD
  · Tests de charge

PHASE 6 — Finalisation (Semaine 13-14)
  · Revue de securite (CORS, rate limiting, injection)
  · Documentation utilisateur
  · Documentation technique API
  · Demo finale
```

---

## Resume des livrables

| Livrable | Tech | Statut actuel |
|---|---|---|
| Pipeline Spark/Airflow | Python + PySpark | Existant — a migrer MongoDB |
| Base de donnees | MongoDB Atlas | A creer |
| API REST | FastAPI + JWT | A creer |
| Dashboard analytique | Streamlit | Existant — a adapter |
| Demo TDR | Streamlit | Existant — corrige |
| Plateforme utilisateur | React + Tailwind | A creer |
| Back-office admin | React | A creer |
| Hebergement | Render + Atlas + Vercel | A deployer |

---

Par quel chantier on commence — la migration MongoDB (Chantier 1) ou l'API FastAPI (Chantier 3) ?

# cluster MongoDB Atlas
mongodb+srv://id_immo_user:IdImmo+228@id-immobilier-cluster.rbaz4yu.mongodb.net/?appName=id-immobilier-cluster