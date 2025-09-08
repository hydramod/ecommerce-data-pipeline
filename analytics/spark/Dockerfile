FROM bitnami/spark:3.5.2
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Kafka (match Spark 3.5.2)
RUN curl -fsSL -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.2/spark-sql-kafka-0-10_2.12-3.5.2.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.2/spark-token-provider-kafka-0-10_2.12-3.5.2.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/kafka-clients-3.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Delta Lake (aligned)
RUN curl -fsSL -o /opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

# S3 support (aligned)
RUN curl -fsSL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chmod 644 /opt/bitnami/spark/jars/*.jar

USER 1001
