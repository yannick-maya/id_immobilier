On repart proprement ! Voici l'ordre complet :

---

## Étape 1 — Tout arrêter

```powershell
docker-compose down
```

---

## Étape 2 — Tout redémarrer

```powershell
docker-compose up -d
```

Attends 30 secondes.

---

## Étape 3 — Recréer l'utilisateur Airflow

```powershell
docker exec -it id_immobilier_airflow airflow users create --username admin --password admin1234 --firstname Admin --lastname User --role Admin --email admin@idimmobilier.tg
```

---

## Étape 4 — Copier le DAG

```powershell
docker cp dags/dag_immobilier.py id_immobilier_airflow:/opt/airflow/dags/dag_immobilier.py
```

---

## Étape 5 — Vérifier que tout tourne

```powershell
docker ps
```

Tu dois voir 4 conteneurs verts :
- `id_immobilier_spark`
- `id_immobilier_spark_worker`
- `id_immobilier_airflow`
- `id_immobilier_streamlit`

---

## Étape 6 — Lancer le pipeline

- Va sur `http://127.0.0.1:8081`
- Connecte-toi `admin / admin1234`
- Clique **une seule fois** sur Trigger DAG
- Surveille `http://127.0.0.1:8082` pour Spark

Dis-moi quand tu es à l'étape 6 !