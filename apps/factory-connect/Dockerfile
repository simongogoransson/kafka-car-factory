FROM quay.io/debezium/connect:3.0 AS debezium

FROM quay.io/strimzi/kafka:0.51.0-kafka-4.1.0

USER root
# Copy JDBC sink plugin from Debezium image into Strimzi Connect plugin path.
COPY --from=debezium /kafka/connect/debezium-connector-jdbc /opt/kafka/plugins/debezium-connector-jdbc
RUN chown -R 1001:0 /opt/kafka/plugins/debezium-connector-jdbc
USER 1001

