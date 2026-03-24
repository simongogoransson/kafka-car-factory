# Schema Registry Helm Chart Migration

## What Was Done

Converted Schema Registry from raw Kubernetes manifests to a proper Helm chart.

### Created Files

```
helm/schema-registry/
├── Chart.yaml                    # Chart metadata
├── values.yaml                   # Default configuration values
├── README.md                     # Chart documentation
└── templates/
    ├── _helpers.tpl              # Helm template helpers
    ├── deployment.yaml           # Deployment template
    └── service.yaml              # Service template
```

### Key Features

✅ **Configurable via values.yaml** - Easy to customize without editing templates  
✅ **Init container** - Waits for Kafka to be ready before starting  
✅ **Health probes** - Liveness and readiness probes included  
✅ **Resource limits** - CPU and memory limits configured  
✅ **Proper labels** - Helm best practices for labels and selectors  

### Configuration Options

All configuration is in `values.yaml`:
- Image version
- Kafka bootstrap servers
- Replication factor
- Resource limits
- Probe timings
- Debug mode

### Skaffold Integration

The Helm chart is automatically deployed by Skaffold:

```yaml
manifests:
  helm:
    releases:
      - name: schema-registry
        chartPath: helm/schema-registry
        namespace: kafka-car-factory
        createNamespace: false
        wait: true
```

### Benefits Over Raw YAML

1. **Reusable** - Can be deployed to different namespaces easily
2. **Versioned** - Chart version tracking
3. **Upgradeable** - `helm upgrade` for updates
4. **Parameterized** - Override values without editing templates
5. **Standard** - Follows Helm best practices

### Testing the Chart

```bash
# Lint the chart
helm lint helm/schema-registry

# Dry run / template rendering
helm template schema-registry helm/schema-registry --namespace kafka-car-factory

# Install manually (optional - Skaffold does this)
helm install schema-registry helm/schema-registry -n kafka-car-factory
```

### Next Steps

The Schema Registry is now managed via Helm and will be deployed automatically with `make dev`.

