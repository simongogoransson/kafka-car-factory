# Schema Registry - Temporarily Disabled

## Status: Commented Out

The Schema Registry has been temporarily disabled in the Skaffold deployment due to startup issues.

## What Was Done

1. **Commented out in skaffold.yaml**:
   - Helm chart deployment
   - Port-forward configuration
   - Schema registry topic creation

2. **Helm Chart Created** (ready for future use):
   - Located at: `helm/schema-registry/`
   - Includes all proper configuration
   - Can be re-enabled when issues are resolved

3. **Node.js Apps Updated** (to work without Schema Registry):
   - **factory-producer**: Made Schema Registry optional via `USE_SCHEMA_REGISTRY` env var
   - **factory-dashboard**: Made Schema Registry optional via `USE_SCHEMA_REGISTRY` env var
   - Both apps now fall back to plain JSON encoding/decoding when Schema Registry is disabled
   - Apps will log whether Schema Registry is enabled or disabled on startup

4. **Deployment Manifests Updated**:
   - Added `USE_SCHEMA_REGISTRY=false` to both deployments
   - Commented out `SCHEMA_REGISTRY_URL` (ready to uncomment when needed)

## To Re-enable Schema Registry

Uncomment/update these sections:

### 1. In `skaffold.yaml`:

**Manifests section** - uncomment the helm releases:
```yaml
helm:
  releases:
    - name: schema-registry
      chartPath: helm/schema-registry
      namespace: kafka-factory
      createNamespace: false
      wait: true
```

**Port-forward section** - uncomment:
```yaml
- resourceType: Service
  resourceName: schema-registry
  namespace: kafka-factory
  port: 8081
  localPort: 8081
```

**Topic creation** - uncomment in rawYaml section:
```yaml
- k8s/schema-registry/schema-registry-topic.yaml
```

### 2. In `k8s/factory-app/deployment-producer.yaml`:

```yaml
env:
  - name: USE_SCHEMA_REGISTRY
    value: "true"  # Change from false to true
  - name: SCHEMA_REGISTRY_URL
    value: http://schema-registry.kafka-factory.svc.cluster.local:8081  # Uncomment
```

### 3. In `k8s/factory-app/deployment-dashboard.yaml`:

```yaml
env:
  - name: USE_SCHEMA_REGISTRY
    value: "true"  # Change from false to true
  - name: SCHEMA_REGISTRY_URL
    value: http://schema-registry.kafka-factory.svc.cluster.local:8081  # Uncomment
```

## Known Issues

Schema Registry was failing with exit code 1 after the init container successfully waited for Kafka. 
The issue appears to be related to configuration or compatibility between:
- Confluent Platform 7.8.7
- Kafka 3.9.0
- Strimzi operator

## Current Deployment

The factory demo now runs without Schema Registry:
- ✅ Kafka cluster (3 controllers + 3 brokers)
- ✅ Factory Producer
- ✅ Factory Dashboard
- ✅ Kafka UI
- ✅ Prometheus
- ✅ PostgreSQL
- ✅ Kafka Connect
- ❌ Schema Registry (disabled)

## Next Steps (Future)

To debug and fix Schema Registry:
1. Check Kafka broker logs for connection issues
2. Verify topic `_schemas` is created correctly
3. Test Schema Registry manually with kubectl
4. Consider using official Confluent Helm charts
5. Verify Kafka client protocol compatibility

Date: March 18, 2026

