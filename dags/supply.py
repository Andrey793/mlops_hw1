from airflow.sdk import dag, Asset, task
from datetime import datetime, timedelta

labeled_data = Asset("labeled_transactions")

VENV = "/opt/airflow/dag-venv/bin/python"

@dag(schedule=timedelta(minutes=5), start_date=datetime(2026, 10, 10), catchup=False, default_args={"depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1)}, max_active_runs=1)
def update_dataset():
    @task.external_python(python=VENV, expect_airflow=False)
    def create_batch_t():
        from pipeline.supply.create_batch import create_batch
        create_batch()

    @task.external_python(python=VENV, expect_airflow=False)
    def filter_db_t():
        from pipeline.supply.clean import filter_db 
        filter_db()

    @task.external_python(python=VENV, expect_airflow=False, outlets=[labeled_data])
    def add_batch_t():
        from pipeline.supply.add_batch import add_batch
        add_batch()

    create_batch_t() >> filter_db_t() >> add_batch_t()

update_dataset()