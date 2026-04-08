# 🏭 Kafka Car Factory Demo

A Kubernetes-based Kafka demo with an **automotive car factory theme**, using:

- **[Strimzi](https://strimzi.io/)** operator — Kafka on Kubernetes (installed via Helm)
- **KRaft mode** — 3 controllers + 3 brokers (no Zookeeper)
- **[Apicurio Registry](https://www.apicur.io/registry/)** — schema governance and artifact storage (Helm chart)
- **[Kafka Connect](https://kafka.apache.org/documentation/#connect)** — sink `vehicle-completed` events into PostgreSQL
- **PostgreSQL** — analytics table `vehicle_completed_events`
- **[Neo4j](https://neo4j.com/)** — graph database for quality-control relationships
- **[Skaffold](https://skaffold.dev/)** — dev/deploy workflow with Helm support
- **[Kafka UI](https://github.com/provectus/kafka-ui)** — topic/consumer browser
- **[Prometheus](https://prometheus.io/)** — metrics from Strimzi JMX exporter
- **Factory Dashboard** — real-time Node.js WebSocket dashboard

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Kubernetes — namespace: strimzi-operator                    │
│  ┌──────────────┐                                            │
│  │    Strimzi   │                                            │
│  │   Operator   │  (installed via Helm)                      │
│  └──────┬───────┘                                            │
│         │ (watches all namespaces)                           │
└─────────┼──────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────────────────────────────────────────────┐
│  Kubernetes — namespace: kafka-car-factory                       │
│                                                              │
│  ┌──────────────────────────────────┐                       │
│  │      Kafka KRaft Cluster         │                       │
│  │  3 Controllers  +  3 Brokers     │                       │
│  └──────────────────────────────────┘                       │
│                  │                                           │
│          ┌───────────────────────┼────────────────────┐     │
│          ▼                       ▼                     ▼     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────┐│
│  │ Factory Producer │  │    Kafka UI      │  │ Prometheus  ││
│  │  (event gen.)    │  │  :8080           │  │  :9090      ││
│  └──────────────────┘  └──────────────────┘  └─────────────┘│
│          │                                                   │
│          ▼                                                   │
│  ┌───────────────────────────────────────────────────────┐  │
│  │     Factory Dashboard  :3000  (WebSocket :3001)       │  │
│  └───────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## Kafka Topics

| Topic | Partitions | Description |
|-------|-----------|-------------|
| `assembly-line-events` | 6 | Station-by-station assembly updates |
| `quality-control-results` | 3 | QC pass/fail per vehicle |
| `vehicle-completed` | 3 | Vehicle rolls off the line |
| `parts-inventory` | 3 | Parts stock level alerts |
| `engine-production` | 3 | Engine build status |
| `paint-shop-events` | 3 | Paint booth stage events |

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| `podman` | ≥ 4.x | https://podman.io/docs/installation |
| `kubectl` | ≥ 1.27 | https://kubernetes.io/docs/tasks/tools/ |
| `kind` | ≥ 0.23 | `brew install kind` |
| `skaffold` | ≥ 2.x | https://skaffold.dev/docs/install/ |
| `helm` | ≥ 3.x | `brew install helm` |

> **Podman machine (macOS):** make sure the Podman machine is running before any step:
> ```bash
> podman machine init   # first time only
> podman machine start
> ```

---

## Quick Start

### 1. Create a local Kubernetes cluster with Kind + Podman

```bash
make cluster
# or manually:
KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name kafka-car-factory --wait 90s
```

Kind runs each Kubernetes node as a Podman container — no VMs needed.

### 2. Install the Strimzi operator (once)

```bash
make setup
# or directly:
bash scripts/install-strimzi.sh
```

This installs the Strimzi operator via Helm into the `strimzi-operator` namespace. The operator is configured with `watchAnyNamespace=true` to manage Kafka resources in **all namespaces** (including `kafka-car-factory` and any future namespaces). Wait ~60s for it to become ready.

### 3. Start the dev loop

```bash
make dev
# or:
skaffold dev --port-forward
```

Skaffold automatically uses the Podman socket (`DOCKER_HOST`) to build container images. It will:
1. Build the `factory-producer` and `factory-dashboard` images via Podman
2. Deploy all Kubernetes manifests
3. Port-forward all services to localhost

### 4. Open the apps

| App | URL |
|-----|-----|
| 🏭 Factory Dashboard | http://localhost:3000 |
| 🚚 Delivered Cars UI | http://localhost:8085 |
| 🕸 Neo4j Browser | http://localhost:7474 |
| 🧩 Apicurio Registry | http://localhost:8080 |
| 📊 Kafka UI | http://localhost:8081 |
| 📈 Prometheus | http://localhost:9090 |

The dashboard includes demo controls to switch between normal and heavy producer load. In heavy mode, it also requests a broker node pool scale-up via the Strimzi KafkaNodePool resource.

### 5. Verify Apicurio Artifact Registration

After `skaffold dev --port-forward` and once the producer is running, verify that Apicurio contains the expected artifact:

```bash
# Registry system info
curl -s http://localhost:8080/apis/registry/v2/system/info

# Latest artifact content registered by factory-producer
curl -s http://localhost:8080/apis/registry/v2/groups/default/artifacts/vehicle-completed-value/versions/latest
```

---

## Kafka Connect Sink Demo

This repo now includes a Kafka Connect sink that writes the `vehicle-completed` topic into PostgreSQL table `vehicle_completed_events`.

It also includes a Neo4j sink proof of concept for `quality-control-results`, backed by an in-cluster Neo4j server with persistent storage.

After `skaffold dev --port-forward`, verify it with:

```bash
kubectl get kafkaconnector vehicle-completed-postgres-sink -n kafka-car-factory
kubectl get kafkaconnector quality-control-neo4j-sink -n kafka-car-factory
kubectl get pods -n kafka-car-factory | grep -E 'factory-connect|postgres|neo4j'
```

Query PostgreSQL (port-forwarded to `localhost:5433` by Skaffold):

```bash
psql "postgresql://factory:factory@localhost:5433/factory_analytics" \
  -c "SELECT vin, model, production_line, event_ts FROM vehicle_completed_events ORDER BY id DESC LIMIT 10;"
```

Open Neo4j Browser at `http://localhost:7474` and sign in with `neo4j` / `factory-graph`, then try:

```cypher
MATCH (v:Vehicle)-[r:HAS_CHECK]->(c:Check)
RETURN v.vin, v.model, c.name, r.result, r.timestamp
ORDER BY r.timestamp DESC
LIMIT 20;
```

```cypher
MATCH (:Vehicle)-[:INSPECTED_BY]->(i:Inspector)
RETURN i.id, count(*) AS inspections
ORDER BY inspections DESC;
```

---

## Project Structure

```
kafka-skaffold/
├── skaffold.yaml                   # Skaffold build + deploy config
├── Makefile                        # Helper targets
├── scripts/
│   └── install-strimzi.sh          # One-time Strimzi bootstrap
├── k8s/
│   ├── namespace.yaml
│   ├── kafka/
│   │   ├── kafka-metrics-cm.yaml   # Prometheus JMX metrics config
│   │   ├── kafka-nodepool-controllers.yaml  # 3 KRaft controllers
│   │   ├── kafka-nodepool-brokers.yaml      # 3 Kafka brokers
│   │   ├── kafka-cluster.yaml      # Kafka CR (KRaft mode)
│   │   ├── kafka-topics.yaml       # 6 factory topics
│   │   └── kafka-connect-topics.yaml # Connect internal topics
│   ├── kafka-connect/
│   │   ├── kafka-connect.yaml      # KafkaConnect cluster
│   │   └── kafka-connector-vehicle-completed-postgres.yaml
│   │   └── kafka-connector-quality-control-neo4j.yaml
│   ├── neo4j/
│   │   ├── neo4j-secret.yaml       # Neo4j credentials for local dev
│   │   └── neo4j.yaml              # Neo4j PVC + Deployment + Service
│   ├── postgres/
│   │   └── postgres.yaml           # Postgres + init SQL table
│   ├── monitoring/
│   │   ├── prometheus-rbac.yaml
│   │   ├── prometheus-config.yaml
│   │   └── prometheus.yaml
│   ├── kafka-ui/
│   │   └── kafka-ui.yaml
│   └── factory-app/
│       ├── deployment-producer.yaml
│       ├── deployment-dashboard.yaml
│       ├── service-dashboard.yaml
│       ├── deployment-delivered-cars.yaml
│       └── service-delivered-cars.yaml
└── apps/
    ├── factory-producer/            # KafkaJS event generator
    │   ├── Dockerfile
    │   ├── package.json
    │   └── src/
    │       ├── index.js             # Producer main loop
    │       └── events.js            # Event generators per topic
    └── factory-dashboard/           # Express + WebSocket dashboard
        ├── Dockerfile
        ├── package.json
        └── src/
            ├── index.js             # Express HTTP server
            ├── consumer.js          # KafkaJS consumer → WS broadcast
            └── public/
                └── index.html       # Live event dashboard
    └── factory-delivered-cars/      # Spring Boot UI for delivered cars from Postgres
      ├── Dockerfile
      ├── pom.xml
      └── src/main/
        ├── java/com/factory/deliveredcars/
        └── resources/templates/index.html
```

---

## Useful Commands

```bash
# Watch all pods
kubectl get pods -n kafka-car-factory -w

# Watch operator
kubectl get pods -n strimzi-operator -w

# All resources
make status

# Stream logs
make logs

# Tear down (keeps operator running)
make clean

# Complete teardown (including operator)
kubectl delete namespace kafka-car-factory strimzi-operator
# Or uninstall via Helm:
helm uninstall strimzi-operator -n strimzi-operator
```

**Note:** `make clean` only deletes the `kafka-car-factory` namespace, preserving the Strimzi operator in `strimzi-operator`. This allows you to run `make dev` again without reinstalling the operator. To completely remove everything including the operator, use `helm uninstall strimzi-operator -n strimzi-operator` or delete both namespaces.

---

## Prometheus Metrics

Strimzi exposes JMX metrics on port **9404** of each broker pod via the Prometheus JMX exporter. Prometheus scrapes these automatically using Kubernetes pod discovery with `prometheus.io/scrape: "true"` annotations.

Key metrics available:
- `kafka_server_brokertopicmetrics_messagesinpersec_total` — messages/sec per topic
- `kafka_server_replicamanager_underreplicatedpartitions` — replication health
- `kafka_network_requestmetrics_requestspersec_total` — request throughput
- `kafka_controller_*` — KRaft controller metrics
- `factory_producer_load_profile{profile="heavy|normal"}` — active demo load mode on the producer
- `factory_producer_events_sent_total` — total producer events emitted during the demo

---

## Notes

- Kafka version: **4.1.0** (KRaft, no Zookeeper)
- Strimzi version: **0.45.0**
- All services are `ClusterIP` — access via Skaffold port-forward
- Storage: `persistent-claim` with `deleteClaim: true` (deleted with namespace)
- The producer generates ~1 event every 300ms–2.5s across all topics
- Skaffold builds images using the **Podman socket** via `DOCKER_HOST` — no Docker daemon needed
- The Makefile auto-detects the Podman socket path via `podman info`
- PostgreSQL sink table: `vehicle_completed_events` via Kafka Connect JDBC sink
