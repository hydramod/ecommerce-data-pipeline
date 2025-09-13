# =========================
# Stage 1: Resolve Hive 3.1.3 client jars once (no Ivy in runtime)
# =========================
FROM debian:12-slim AS hive3-resolver

ARG SPARK_VERSION=3.5.2
ARG HADOOP_MAJOR=3
ARG HIVE_CLIENT_VERSION=3.1.3

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates openjdk-17-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install Spark (same version as runtime) just for resolution
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}" "${SPARK_HOME}" && \
    rm -f /tmp/spark.tgz

# Trigger Maven/Ivy resolution for Hive 3.1.3 (connects to localhost; command will "fail" after download, which is fine)
RUN /opt/spark/bin/spark-sql \
      --conf spark.sql.hive.metastore.jars=maven \
      --conf spark.sql.hive.metastore.version=${HIVE_CLIENT_VERSION} \
      --conf spark.jars.repositories=https://repo1.maven.org/maven2 \
      --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 \
      -S -e "SHOW DATABASES;" || true

# Collect resolved jars into /tmp/hive3 (covers common Ivy layouts)
RUN mkdir -p /tmp/hive3 && \
    find /root/.ivy2/cache -type f -path "*/org.apache.hive/*/jars/*.jar" -exec cp -n {} /tmp/hive3/ \; || true && \
    find /root/.ivy2/cache -type f -path "*/org.apache.thrift/*/jars/*thrift*.jar" -exec cp -n {} /tmp/hive3/ \; || true && \
    find /root/.ivy2/cache -type f -path "*/org.apache.thrift/*/jars/*fb303*.jar" -exec cp -n {} /tmp/hive3/ \; || true && \
    find /root/.ivy2/cache -type f -path "*/org.apache.curator/*/jars/*.jar" -exec cp -n {} /tmp/hive3/ \; || true && \
    find /root/.ivy2/cache -type f -path "*/org.apache.zookeeper/*/jars/*.jar" -exec cp -n {} /tmp/hive3/ \; || true && \
    cp -n /root/.ivy2/jars/*.jar /tmp/hive3/ 2>/dev/null || true && \
    echo "---- Resolved Hive client jars ----" && ls -1 /tmp/hive3 | sort

# Soft sanity (do not fail build if name differs)
RUN bash -lc 'if ls /tmp/hive3/*metastore-${HIVE_CLIENT_VERSION}.jar >/dev/null 2>&1; then \
  echo "Metastore jar present:"; ls -1 /tmp/hive3/*metastore-${HIVE_CLIENT_VERSION}.jar; \
else echo "WARNING: metastore jar filename differs; using wildcard classpath."; fi'

# =========================
# Stage 2: Airflow runtime (driver lives here) — NO Ivy at runtime
# =========================
# Airflow 3.0.5 base (Python 3.12)
FROM bitnami/airflow:3.0.5
USER root

ARG SPARK_VERSION=3.5.2
ARG HADOOP_MAJOR=3
ARG HIVE_CLIENT_VERSION=3.1.3

RUN install_packages curl ca-certificates openjdk-17-jre-headless netcat-openbsd procps

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install Spark client
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR}" "${SPARK_HOME}" && \
    rm -f /tmp/spark.tgz

# --- Your original extra JVM libs (UNCHANGED) ---
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

# >>> Copy the fully-resolved Hive 3.1.3 client jars into the driver image <<<
COPY --from=hive3-resolver /tmp/hive3 /opt/spark/hive3

# Python libs
RUN /opt/bitnami/airflow/venv/bin/pip install --no-cache-dir \
      "apache-airflow-providers-apache-spark>=5.3.2" \
      "apache-airflow-providers-common-compat>=1.5.0" \
      "pyspark==3.5.2" \
      "delta-spark==3.2.0" \
      "graphviz" \
      "grpcio-status>=1.59.0" \
      "dbt-core>=1.8,<1.10" \
      "dbt-trino>=1.8,<1.10"

ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS="False"
