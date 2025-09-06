# Airflow 3.0.5 base (Python 3.12)
FROM bitnami/airflow:3.0.5
USER root

ARG SPARK_VERSION=3.5.2
ARG HADOOP_MAJOR=3

RUN install_packages curl ca-certificates openjdk-17-jre-headless netcat-openbsd procps

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install Spark client
RUN set -euo pipefail && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}" "${SPARK_HOME}" && \
    rm -f /tmp/spark.tgz

# --- Add JVM libs needed by the driver ---
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.2/spark-sql-kafka-0-10_2.12-3.5.2.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.2/spark-token-provider-kafka-0-10_2.12-3.5.2.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    # Delta (match your spark.sql.extensions)
    curl -fsSL -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL -o /opt/spark/jars/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    # S3 support (only if driver touches s3a://)
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar


# Python libs – pin to the same Spark line
RUN /opt/bitnami/airflow/venv/bin/pip install --no-cache-dir \
      "apache-airflow-providers-apache-spark>=5.3.2" \
      "apache-airflow-providers-common-compat>=1.5.0" \
      "pyspark==3.5.2" \
      "delta-spark==3.2.0" \
      "graphviz" \
      "grpcio-status>=1.59.0"

ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS="False"
