"""
DAG Apache Airflow — ID Immobilier
Orchestration complète du pipeline de données
Fréquence : hebdomadaire (chaque lundi à 6h00)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

# ─── Configuration du DAG ──────────────────────────────────────────────────────
default_args = {
    "owner": "id_immobilier",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="id_immobilier_pipeline",
    default_args=default_args,
    description="Pipeline complet ID Immobilier — Ingestion → Cleaning → MySQL → Indicateurs → Indice",
    schedule_interval="0 6 * * 1",  # Chaque lundi à 6h00
    catchup=False,
    tags=["immobilier", "togo", "data-pipeline"],
)

# ─── Fonctions wrappées pour Airflow ───────────────────────────────────────────
def run_ingestion():
    from pipeline.ingestion import ingest
    ingest()

def run_cleaning():
    subprocess.run(
        ["spark-submit", "--master", "local[*]", "pipeline/cleaning.py"],
        check=True
    )

def run_modeling():
    from pipeline.modeling import run
    run()

def run_indicators():
    from pipeline.indicators import run
    run()

def run_index():
    from pipeline.index import run
    run()

# ─── Définition des tâches ─────────────────────────────────────────────────────
task_ingestion = PythonOperator(
    task_id="ingestion_donnees",
    python_callable=run_ingestion,
    dag=dag,
)

task_cleaning = PythonOperator(
    task_id="cleaning_pyspark",
    python_callable=run_cleaning,
    dag=dag,
)

task_modeling = PythonOperator(
    task_id="modeling_mysql",
    python_callable=run_modeling,
    dag=dag,
)

task_indicators = PythonOperator(
    task_id="calcul_indicateurs",
    python_callable=run_indicators,
    dag=dag,
)

task_index = PythonOperator(
    task_id="calcul_indice",
    python_callable=run_index,
    dag=dag,
)

# ─── Ordre d'exécution ─────────────────────────────────────────────────────────
task_ingestion >> task_cleaning >> task_modeling >> task_indicators >> task_index
