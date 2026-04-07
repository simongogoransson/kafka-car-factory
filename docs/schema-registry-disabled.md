# Apicurio Registry - Temporarily Disabled

## Status: Commented Out

Apicurio Registry is currently disabled in the Skaffold deployment while the base migration is being finalized.

## Current Setup

1. Skaffold entries are kept commented in `skaffold.yaml` for:
   - Helm release `apicurio-registry`
   - Service port-forward on `8080`

2. Helm chart is ready in `helm/apicurio-registry/`.

3. Node.js apps are registry-aware but keep JSON payload behavior:
   - `factory-producer` uses `USE_APICURIO_REGISTRY`
   - `factory-dashboard` uses `USE_APICURIO_REGISTRY`

4. Deployments default to disabled mode:
   - `USE_APICURIO_REGISTRY=false`
   - `APICURIO_REGISTRY_URL` left commented

## Re-enable Apicurio Registry

### 1. In skaffold.yaml

Uncomment the Helm release:

```yaml
helm:
  releases:
    - name: apicurio-registry
      chartPath: helm/apicurio-registry
      namespace: kafka-car-factory
      createNamespace: false
      wait: true
```

Uncomment the service port-forward:

```yaml
- resourceType: Service
  resourceName: apicurio-registry
  namespace: kafka-car-factory
  port: 8080
  localPort: 8080
```

### 2. In k8s/factory-app/deployment-producer.yaml

```yaml
env:
  - name: USE_APICURIO_REGISTRY
    value: "true"
  - name: APICURIO_REGISTRY_URL
    value: http://apicurio-registry.kafka-car-factory.svc.cluster.local:8080
```

### 3. In k8s/factory-app/deployment-dashboard.yaml

```yaml
env:
  - name: USE_APICURIO_REGISTRY
    value: "true"
  - name: APICURIO_REGISTRY_URL
    value: http://apicurio-registry.kafka-car-factory.svc.cluster.local:8080
```

## Verification Checklist

1. `kubectl get pods -n kafka-car-factory | grep apicurio-registry`
2. `kubectl port-forward svc/apicurio-registry 8080:8080 -n kafka-car-factory`
3. `curl -I http://localhost:8080/`
4. Confirm producer logs show successful artifact registration.

