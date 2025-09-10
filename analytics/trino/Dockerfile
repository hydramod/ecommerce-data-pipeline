FROM trinodb/trino:476

# Add Hadoop S3A + AWS SDK jars so the register_table procedure can use s3a://
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /usr/lib/trino/plugin/delta-lake/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /usr/lib/trino/plugin/delta-lake/
