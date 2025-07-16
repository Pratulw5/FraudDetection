# Base image
FROM flink:1.18

ENV FLINK_CONNECTOR_KAFKA_VERSION=3.2.0-1.18
ENV KAFKA_CLIENT_VERSION=3.6.1

# Download Flink Kafka connector
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_CONNECTOR_KAFKA_VERSION}/flink-connector-kafka-${FLINK_CONNECTOR_KAFKA_VERSION}.jar \
    -O /opt/flink/lib/flink-connector-kafka-${FLINK_CONNECTOR_KAFKA_VERSION}.jar

# Download Kafka client
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar \
    -O /opt/flink/lib/kafka-clients-${KAFKA_CLIENT_VERSION}.jar

# Copy SQL job
COPY ./FlinkJob /opt/flink/jobs/

# Automatically run the SQL job on start
ENTRYPOINT ["/bin/bash", "-c", "/opt/flink/bin/sql-client.sh -f /opt/flink/jobs/suspicioustransactionfilter.sql && /docker-entrypoint.sh jobmanager"]
