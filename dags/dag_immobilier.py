"""
DAG Apache Airflow - ID Immobilier
Orchestration complete du pipeline de donnees
Frequence : hebdomadaire (chaque lundi a 6h00)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

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
    description="Pipeline ID Immobilier - Ingestion -> Cleaning Spark -> Modeling -> Indicateurs -> Indice",
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

# ── Tache 2a : Nettoyage fichiers precedents ──────────────────────────────────
task_nettoyage = BashOperator(
    task_id="nettoyage_precedent",
    bash_command=(
        "rm -f /opt/airflow/data/cleaned_v2/annonces_clean.csv && "
        "rm -f /opt/airflow/data/raw/rejets/annonces_rejetees.csv && "
        "echo 'Anciens fichiers pandas supprimes'"
    ),
    dag=dag,
)

# ── Tache 2b : Nettoyage V2 avec Spark via docker exec ───────────────────────
# Airflow n'a pas spark-submit — on soumet le job via le conteneur Spark
def run_cleaning_spark():
    import subprocess
    result = subprocess.run(
        [
            "docker", "exec", "id_immobilier_spark",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark:7077",
            "/app/pipeline/cleaning_v2.py"
        ],
        capture_output=True, text=True, timeout=600
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Spark job failed with code {result.returncode}")
    print("Spark cleaning termine avec succes")

task_cleaning = PythonOperator(
    task_id="cleaning_pyspark_v2",
    python_callable=run_cleaning_spark,
    dag=dag,
    execution_timeout=timedelta(minutes=15),
)

# ── Tache 3 : Modelisation V2 ─────────────────────────────────────────────────
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
task_ingestion >> task_nettoyage >> task_cleaning >> task_modeling >> task_indicators >> task_index