FROM flink:1.19.1-scala_2.12-java17

# Kafka SQL connector (flink-json, which provides debezium-json, ships in lib/ already).
ADD --chown=flink:flink \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar \
    /opt/flink/lib/

COPY --chown=flink:flink jobs/ /opt/flink/jobs/

# Named volume mounted here inherits this ownership on first use (checkpoints/savepoints).
RUN mkdir -p /flink-data && chown flink:flink /flink-data
