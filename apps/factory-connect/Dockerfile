# Custom Kafka Connect image for local Kind development.
#
# Strimzi requires its own base image (quay.io/strimzi/kafka) to run Kafka Connect.
# The plain Debezium image cannot be used directly — it uses a different entrypoint
# and filesystem layout that Strimzi does not support.
#
# The Strimzi build feature (spec.build in KafkaConnect) is the cloud-native alternative,
# but it requires a container registry to push the built image to — which a local Kind
# cluster does not have by default.
#
# This Dockerfile is therefore the simplest reliable approach for local development:
#   1. Use the Strimzi Kafka image as the base (correct entrypoint + layout)
#   2. Copy the Debezium JDBC sink plugin from the Debezium image into the Strimzi plugin path
#   3. Download the Confluent Avro converter so the JDBC sink can deserialize Avro messages
#      produced by the factory-producer via the Confluent Schema Registry.
#
# Skaffold builds this image and loads it directly into the Kind cluster via `kind load`,
# so no external registry is needed.
FROM quay.io/debezium/connect:3.0 AS debezium

FROM quay.io/strimzi/kafka:0.51.0-kafka-4.1.0

USER root

# Copy JDBC sink plugin from Debezium image into Strimzi Connect plugin path.
COPY --from=debezium /kafka/connect/debezium-connector-jdbc /opt/kafka/plugins/debezium-connector-jdbc

# Download the Confluent Avro converter (kafka-connect-avro-converter) so Connect
# can deserialize Avro messages encoded with the Confluent wire format.
RUN curl -fsSL \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.6.1/kafka-connect-avro-converter-7.6.1.jar \
    -o /opt/kafka/plugins/debezium-connector-jdbc/kafka-connect-avro-converter.jar && \
    curl -fsSL \
    https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.6.1/kafka-avro-serializer-7.6.1.jar \
    -o /opt/kafka/plugins/debezium-connector-jdbc/kafka-avro-serializer.jar && \
    curl -fsSL \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.6.1/kafka-schema-registry-client-7.6.1.jar \
    -o /opt/kafka/plugins/debezium-connector-jdbc/kafka-schema-registry-client.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar \
    -o /opt/kafka/plugins/debezium-connector-jdbc/avro.jar

RUN chown -R 1001:0 /opt/kafka/plugins/debezium-connector-jdbc

USER 1001
