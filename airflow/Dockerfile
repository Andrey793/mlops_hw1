FROM apache/airflow:3.2.1-python3.12
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow/dag-venv \
 && /opt/airflow/dag-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
