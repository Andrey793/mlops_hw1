from airflow.sdk import dag, Asset, task
from datetime import datetime, timedelta

VENV = "/opt/airflow/dag-venv/bin/python"

labeled_data = Asset("labeled_transactions")
@dag(schedule=[labeled_data], start_date=datetime(2026, 10, 10), catchup=False, default_args={"depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1)}, max_active_runs=1)
def retrain_catboost():
    @task.external_python(python=VENV, expect_airflow=False)
    def build_dataset_t():
        from pipeline.retrain.preproc import build_dataset
        build_dataset()

    @task.external_python(python=VENV, expect_airflow=False)
    def train_t():
        from pipeline.retrain.train import train
        train()

    @task.external_python(python=VENV, expect_airflow=False)
    def publish_t():
        from pipeline.retrain.publish import publish
        publish()

    build_dataset_t() >> train_t() >> publish_t()

retrain_catboost()