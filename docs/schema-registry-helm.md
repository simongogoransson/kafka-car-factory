# Apicurio Registry Helm Chart Migration

## What Was Done

Migrated the previous schema-registry chart to a native Apicurio Registry chart.

### Chart Path

```
helm/apicurio-registry/
├── Chart.yaml
├── values.yaml
├── README.md
└── templates/
    ├── _helpers.tpl
    ├── deployment.yaml
    └── service.yaml
```

### Key Changes

1. Chart renamed to `apicurio-registry`.
2. Runtime image changed to `apicurio/apicurio-registry`.
3. Storage configured as `kafkasql` with Kafka bootstrap servers.
4. Health probes updated to Quarkus health endpoints.
5. Legacy schema-registry environment variables removed.

### Skaffold Integration

Use this release block when enabling the registry:

```yaml
manifests:
  helm:
    releases:
      - name: apicurio-registry
        chartPath: helm/apicurio-registry
        namespace: kafka-car-factory
        createNamespace: false
        wait: true
```

### Testing the Chart

```bash
helm lint helm/apicurio-registry
helm template apicurio-registry helm/apicurio-registry --namespace kafka-car-factory
helm install apicurio-registry helm/apicurio-registry -n kafka-car-factory
```

