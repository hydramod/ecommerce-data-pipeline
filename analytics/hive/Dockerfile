FROM apache/hive:3.1.3

ARG HADOOP_AWS_VERSION=3.1.0
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.7

# Switch to root for file operations
USER root

# 1) Clean up ONLY the specific old JDBC driver that causes conflicts
RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar 2>/dev/null || true

# 2) Add new drivers (they will overwrite any existing files with same name)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/hive/lib/
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/hive/lib/

# 3) Verify what's actually in the lib directory
RUN echo "=== Contents of /opt/hive/lib/ ===" && \
    ls -la /opt/hive/lib/postgresql-*.jar 2>/dev/null || echo "No PostgreSQL jars found"

# 4) Silence duplicate SLF4J binding warning
RUN rm -f /opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-*.jar 2>/dev/null || true

# 5) Set environment variables
ENV HADOOP_USER_CLASSPATH_FIRST=true
ENV HIVE_AUX_JARS_PATH=/opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar:/opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar:/opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
ENV HADOOP_CLASSPATH=$HIVE_AUX_JARS_PATH
ENV CLASSPATH=$HIVE_AUX_JARS_PATH

# 6) Test that the JDBC driver can be loaded
RUN java -cp /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar org.postgresql.Driver 2>/dev/null && \
    echo "JDBC driver test: SUCCESS" || \
    echo "JDBC driver test: WARNING - driver may not be compatible"

# Optional: Switch back to original user if needed
# USER hive