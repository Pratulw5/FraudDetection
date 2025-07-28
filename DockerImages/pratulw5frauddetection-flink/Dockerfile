# Use the official Flink image as base
FROM flink:1.18

ENV FLINK_CONNECTOR_KAFKA_VERSION=3.2.0-1.18
ENV KAFKA_CLIENT_VERSION=3.6.1
# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir --upgrade pip

# Install required Python libraries including pyflink
RUN pip install --no-cache-dir pyflink river redis psycopg2-binary

# Download Kafka connector
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_CONNECTOR_KAFKA_VERSION}/flink-connector-kafka-${FLINK_CONNECTOR_KAFKA_VERSION}.jar \
    -O /opt/flink/lib/flink-connector-kafka-${FLINK_CONNECTOR_KAFKA_VERSION}.jar

# Download Kafka client dependency
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar \
    -O /opt/flink/lib/kafka-clients-${KAFKA_CLIENT_VERSION}.jar

# Verify
RUN ls /opt/flink/lib

ENTRYPOINT ["/docker-entrypoint.sh"]
