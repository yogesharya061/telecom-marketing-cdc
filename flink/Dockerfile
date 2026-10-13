FROM flink:1.20.1-scala_2.12-java17
USER root
RUN curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar -o /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
 && mkdir -p /opt/flink/checkpoints \
 && chown -R flink:flink /opt/flink/checkpoints
USER flink
