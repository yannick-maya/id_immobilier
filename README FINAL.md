# ID Immobilier - README FINAL

## 1. Contexte

Projet de plateforme immobilière pour Togo :
- Backend API FastAPI + MongoDB
- Frontend utilisateur React + Tailwind
- Back-office admin React
- Pipeline Spark/Airflow + Streamlit

Ce README couvre tout le workflow de démarrage, test et production.

---

## 2. Structure du repo

- `api/` : FastAPI, routers, auth, modèles
- `frontend/` : SPA utilisateur
- `admin/` : SPA back-office admin
- `dashboard/` : Streamlit analytics
- `pipeline/` : scripts ETL / nettoyage
- `data/` : fichiers source / nettoyés
- `docker-compose.yml` : services Spark, Airflow, Mongo, API, frontend, admin.
- `TODO-Version_2.md` : feuille de route complète

---

## 3. Pré-requis locaux

- Git
- Python 3.10+ (Anaconda supporté)
- Node.js 18+ / npm
- MongoDB local ou Atlas
- Docker & Docker Compose (optionnel pour production)

---

## 4. Installation backend (local)

1. Ouvrir terminal dans `id_immobilier/`
2. Créer venv (recommandé) :
   - `python -m venv .venv`
   - Windows: `.\.venv\Scripts\activate`
   - Mac/Linux: `source .venv/bin/activate`
3. Installer dépendances Python : `pip install -r requirements.txt`
4. Ajouter dépendances manquantes (motor / passlib) :
   - `pip install motor passlib[bcrypt] python-jose[cryptography]`
5. Préparer `.env` (copier `.env.example`)
6. Démarrer : `uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`

Endpoints clés :
- `GET /` (status)
- `POST /auth/register`
- `POST /auth/login`
- `/admin/*`, `/annonces`, `/statistiques` etc.

---

## 5. Admin - back-office (dev)

1. Aller dans `id_immobilier/admin`
2. Installer dépendances : `npm install`
3. Lancer : `npm start`
4. Naviguer vers `http://localhost:3000`

**Note** : pour utiliser les APIs protégées, faites login admin via endpoint `/auth/login`.
- Si pas d’admin : créer user via `/auth/register`, puis faire `PUT /admin/users/{id}?role=admin` depuis API ou Mongo.

Ce back-office a été développé dans `admin/src/App.js` et consomme :
- `/admin/stats`, `/admin/users`, `/admin/annonces`, `/admin/okr`
- `/admin/users/{id}` (PUT, role)
- `/admin/annonces/{id}/valider`, `/admin/annonces/{id}/refuser`

---

## 6. Frontend utilisateur (dev)

1. Aller dans `id_immobilier/frontend`
2. `npm install`
3. `npm start`
4. Ouvrir `http://localhost:3000`

---

## 7. Production / Docker (chantier 6)

1. Installer Docker & Docker Compose.
2. Dans le dossier racine `id_immobilier` :
   - `docker compose up --build`
3. Services exposés :
   - API : `http://localhost:8000`
   - Frontend : `http://localhost:3000`
   - Admin : `http://localhost:3001`
   - Streamlit : `http://localhost:8501`
   - Airflow : `http://localhost:8081`

---

## 8. Route de déploiement Render (suggestion)

- API FastAPI : `Dockerfile.api` + service `id-immobilier-api`.
- Frontend utilisateur : `frontend/Dockerfile` + service.
- Admin : `admin/Dockerfile` + service.
- MongoDB : MongoDB Atlas (à préférer à service local).
- Env JSON sur Render : `MONGO_URI`, `MONGO_DB`, `SECRET_KEY`.
- Activer `Dockerfile` via build command.

---

## 9. Notes sur correctifs réalisés

- `api/auth/middleware.py` : conversion du `sub` JWT en `ObjectId` pour le lookup.
- `api/auth/password.py` : normalisation + coupe à 72 bytes (bcrypt).
- `api/routers/users.py` : login via `UserLogin` (body JSON), gestion des exceptions et `token` retourne user.
- `api/routers/admin.py` : endpoints `admin/users`, `admin/annonces`, `admin/stats`, `admin/okr` valides.
- `admin/` : nouveau code App.js + login/dashboard/users/annonces/okr.

---

## 10. Checklist chantier 5 admin

- [x] API admin existante (`/admin/*`).
- [x] Auth `get_current_admin`. 
- [x] CRUD users + annonces + stats + OKR.
- [x] Interface React admin implementée.
- [ ] `npm install` check (noeud non installé sur environnement de test courant).

---

## 11. Problèmes rencontrés / opérations échouées

- 1) `npm install` dans `admin/` n’a pas créé `node_modules` dans cet environnement (tentatives : 2). Relancer localement.
- 2) `pip install motor` échoué initialement via conda, puis réussi via pip en local.
- 3) Registration `login` via API échouait avant correction de l’auth middleware (maintenant OK localement).

---

## 12. Phase 6 (finalisation)

- Tests unitaires (pytest) pour `api/routers/*` et `admin`.
- CORS + rate limiting + sécurité JWT + RBAC.
- Documentation Postman / OpenAPI + API reference.
- Déploiement final Render + CI GitHub Actions.
