# Schema Registry Helm Chart

This Helm chart deploys Confluent Schema Registry for Kafka.

## Configuration

The following table lists the configurable parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of Schema Registry replicas | `1` |
| `image.repository` | Schema Registry image repository | `confluentinc/cp-schema-registry` |
| `image.tag` | Schema Registry image tag | `7.8.7` |
| `kafka.bootstrapServers` | Kafka bootstrap servers | `factory-kafka-kafka-bootstrap.kafka-car-factory.svc.cluster.local:9092` |
| `kafka.topicReplicationFactor` | Replication factor for _schemas topic | `3` |
| `waitForKafka.enabled` | Enable init container to wait for Kafka | `true` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `8081` |
| `resources.requests.memory` | Memory request | `256Mi` |
| `resources.requests.cpu` | CPU request | `100m` |
| `resources.limits.memory` | Memory limit | `512Mi` |
| `resources.limits.cpu` | CPU limit | `300m` |

## Example Values Override

Create a custom `values.yaml`:

```yaml
replicaCount: 2

kafka:
  bootstrapServers: "my-kafka-cluster:9092"
  topicReplicationFactor: 1

resources:
  limits:
    memory: 1Gi
```

## Installation

```bash
helm install schema-registry ./helm/schema-registry \
  --namespace kafka-car-factory \
  --create-namespace
```

## Upgrade

```bash
helm upgrade schema-registry ./helm/schema-registry \
  --namespace kafka-car-factory
```

## Uninstall

```bash
helm uninstall schema-registry -n kafka-car-factory
```

