"""
DAG Apache Airflow - ID Immobilier
Orchestration complete du pipeline de donnees
Frequence : hebdomadaire (chaque lundi a 6h00)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

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
    description="Pipeline ID Immobilier - Ingestion -> Cleaning V2 -> Modeling V2 -> Indicateurs -> Indice",
    schedule_interval="0 6 * * 1",
    catchup=False,
    tags=["immobilier", "togo", "big-data"],
)

# ── Tache 1 : Ingestion ───────────────────────────────────────────────────────
def run_ingestion():
    sys.path.insert(0, "/opt/airflow")
    from pipeline.ingestion import ingest
    ingest()

task_ingestion = PythonOperator(
    task_id="ingestion_donnees",
    python_callable=run_ingestion,
    dag=dag,
)

# ── Tache 2 : Nettoyage V2 avec Spark ────────────────────────────────────────
task_cleaning = BashOperator(
    task_id="cleaning_pyspark_v2",
    bash_command="cd /opt/airflow && spark-submit --master spark://spark:7077 pipeline/cleaning_v2.py",
    dag=dag,
)

# ── Tache 3 : Modelisation V2 ────────────────────────────────────────────────
def run_modeling():
    sys.path.insert(0, "/opt/airflow")
    from pipeline.modeling_v2 import run
    run()

task_modeling = PythonOperator(
    task_id="modeling_mysql_v2",
    python_callable=run_modeling,
    dag=dag,
)

# ── Tache 4 : Calcul des indicateurs ─────────────────────────────────────────
def run_indicators():
    sys.path.insert(0, "/opt/airflow")
    from pipeline.indicators import run
    run()

task_indicators = PythonOperator(
    task_id="calcul_indicateurs",
    python_callable=run_indicators,
    dag=dag,
)

# ── Tache 5 : Calcul de l indice ─────────────────────────────────────────────
def run_index():
    sys.path.insert(0, "/opt/airflow")
    from pipeline.index import run
    run()

task_index = PythonOperator(
    task_id="calcul_indice",
    python_callable=run_index,
    dag=dag,
)

# ── Ordre d execution ─────────────────────────────────────────────────────────
task_ingestion >> task_cleaning >> task_modeling >> task_indicators >> task_index