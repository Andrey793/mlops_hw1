FROM apache/airflow:3.2.1-python3.12
USER root                         
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*   
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
USER airflow                      
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_JARS=/opt/spark-jars/postgresql.jar   

COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow/dag-venv \
 && /opt/airflow/dag-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
