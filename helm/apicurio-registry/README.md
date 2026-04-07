# Apicurio Registry Helm Chart

This Helm chart deploys native Apicurio Registry with KafkaSQL storage.

## Configuration

The following table lists the configurable parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of Apicurio Registry replicas | `1` |
| `image.repository` | Registry image repository | `apicurio/apicurio-registry` |
| `image.tag` | Registry image tag | `3.2.1` |
| `kafka.bootstrapServers` | Kafka bootstrap servers | `factory-kafka-kafka-bootstrap.kafka-car-factory.svc.cluster.local:9092` |
| `kafka.storageTopic` | Kafka topic used for registry storage | `apicurio-registry-storage` |
| `kafka.storageTopicReplicationFactor` | Replication factor for the storage topic | `3` |
| `storage.kind` | Registry storage backend | `kafkasql` |
| `waitForKafka.enabled` | Enable init container to wait for Kafka | `true` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | Service port | `8080` |
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
  storageTopicReplicationFactor: 1

resources:
  limits:
    memory: 1Gi
```

## Installation

```bash
helm install apicurio-registry ./helm/apicurio-registry \
  --namespace kafka-car-factory \
  --create-namespace
```

## Upgrade

```bash
helm upgrade apicurio-registry ./helm/apicurio-registry \
  --namespace kafka-car-factory
```

## Uninstall

```bash
helm uninstall apicurio-registry -n kafka-car-factory
```

