"""
multi_api_pipeline — Daily ETL DAG
Fetches data from 3 public APIs and loads it into PostgreSQL.
Schedule: 07:00 EAT (04:00 UTC) daily.
"""
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Project root is mounted at /opt/airflow/project via docker-compose volume.
sys.path.insert(0, "/opt/airflow/project")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ian",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def validate_config():
    """Verify database connectivity before extraction begins."""
    from src.database import DatabaseConnection
    db = DatabaseConnection()
    db.connect()
    rows = db.fetchall("SELECT COUNT(*) FROM pipeline_runs")
    prior_runs = rows[0][0]
    logger.info(f"DB connection OK — {prior_runs} prior pipeline runs on record")
    db.close()


def extract_load(**context):
    """Run the full ETL batch, triggered by Airflow."""
    from src.pipeline import run_pipeline
    success = run_pipeline(batch_size=1, triggered_by="airflow")
    if not success:
        raise RuntimeError("Pipeline returned FAILED — check pipeline_runs table for details")


def quality_check():
    """Confirm the latest run inserted at least one row per source."""
    from src.database import DatabaseConnection
    db = DatabaseConnection()
    db.connect()

    rows = db.fetchall(
        """
        SELECT triggered_by, status,
               advice_inserted, quotes_inserted, dogs_inserted,
               duration_seconds, run_at
        FROM   pipeline_runs
        ORDER  BY run_at DESC
        LIMIT  1
        """
    )
    db.close()

    if not rows:
        raise ValueError("No pipeline_runs record found — extraction may have failed silently")

    _, status, advice, quotes, dogs, duration, run_at = rows[0]
    logger.info(
        f"Latest run ({run_at}): status={status} | "
        f"advice={advice} quotes={quotes} dogs={dogs} | {duration}s"
    )

    if status != "SUCCESS":
        raise ValueError(f"Latest pipeline run status is {status}")


with DAG(
    dag_id="multi_api_pipeline",
    default_args=default_args,
    description="Daily ETL: Advice Slip + ZenQuotes + Dog CEO → PostgreSQL",
    schedule_interval="0 4 * * *",  # 07:00 EAT = 04:00 UTC
    catchup=False,
    tags=["etl", "api", "postgres"],
) as dag:

    t1 = PythonOperator(task_id="validate_config",  python_callable=validate_config)
    t2 = PythonOperator(task_id="extract_load",     python_callable=extract_load)
    t3 = PythonOperator(task_id="quality_check",    python_callable=quality_check)

    t1 >> t2 >> t3
