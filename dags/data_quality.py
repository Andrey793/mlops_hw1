from airflow.sdk import dag, Asset, task
from datetime import datetime, timedelta

VENV = "/opt/airflow/dag-venv/bin/python"

labeled_data = Asset("labeled_transactions")
@dag(schedule=[labeled_data], start_date=datetime(2026, 10, 10), catchup=False, default_args={"depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1)}, max_active_runs=1)
def data_quality():
    @task.external_python(python=VENV, expect_airflow=False)
    def uniqueness_t():
        from pipeline.quality.checks import uniqueness
        return uniqueness()

    @task.external_python(python=VENV, expect_airflow=False)
    def completeness_t():
        from pipeline.quality.checks import completeness
        return completeness()

    @task.external_python(python=VENV, expect_airflow=False)
    def validity_t():
        from pipeline.quality.checks import validity
        return validity()

    @task.external_python(python=VENV, expect_airflow=False)
    def continuity_t():
        from pipeline.quality.checks import continuity
        return continuity()

    @task.external_python(python=VENV, expect_airflow=False)
    def retention_t():
        from pipeline.quality.checks import retention
        return retention()

    @task.external_python(python=VENV, expect_airflow=False)
    def report_t(results):
        from pipeline.quality.report import write_report
        write_report(results)

    r1 = uniqueness_t()
    r2 = completeness_t()
    r3 = validity_t()
    r4 = continuity_t()
    r5 = retention_t()
    report_t([r1, r2, r3, r4, r5])

data_quality()