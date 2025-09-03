# Airflow 3.0.5 base (Python 3.12)
FROM bitnami/airflow:3.0.5

USER root

# ---- Versions (align with your cluster) ----
ARG SPARK_VERSION=3.5.1
ARG HADOOP_MAJOR=3

# ---- System deps & Java runtime ----
# - curl/ca-certificates: download Spark
# - openjdk-17-jre-headless: required by spark-submit
# - netcat-openbsd: handy for quick port checks
RUN install_packages \
      curl \
      ca-certificates \
      openjdk-17-jre-headless \
      netcat-openbsd \
      procps

# ---- Install Apache Spark client (spark-submit) ----
# We only need the client to submit to spark://spark-master:7077
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN set -euo pipefail && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}" "${SPARK_HOME}" && \
    rm -f /tmp/spark.tgz

# ---- Python deps inside Airflow's venv ----
# Versions chosen to match Spark 3.5.x and Airflow provider 5.3.x
RUN /opt/bitnami/airflow/venv/bin/pip install --no-cache-dir \
      "apache-airflow-providers-apache-spark>=5.3.2" \
      "apache-airflow-providers-common-compat>=1.5.0" \
      "pyspark>=3.5.2" \
      "delta-spark==3.2.0" \
      "graphviz" \
      "grpcio-status>=1.59.0"

# Optional: helpful for DAG rendering warnings to go away
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS="False"

# Keep using the same entrypoints/commands from docker-compose
USER 1001
