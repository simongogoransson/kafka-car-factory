# 🏭 Kafka Car Factory Demo

A Kubernetes-based Kafka demo with an **automotive car factory theme**, using:

- **[Strimzi](https://strimzi.io/)** operator — Kafka on Kubernetes
- **KRaft mode** — 3 controllers + 3 brokers (no Zookeeper)
- **[Skaffold](https://skaffold.dev/)** — dev/deploy workflow
- **[Kafka UI](https://github.com/provectus/kafka-ui)** — topic/consumer browser
- **[Prometheus](https://prometheus.io/)** — metrics from Strimzi JMX exporter
- **Factory Dashboard** — real-time Node.js WebSocket dashboard

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Kubernetes — namespace: kafka-factory                       │
│                                                              │
│  ┌──────────────┐    ┌──────────────────────────────────┐   │
│  │    Strimzi   │    │      Kafka KRaft Cluster         │   │
│  │   Operator   │───▶│  3 Controllers  +  3 Brokers     │   │
│  └──────────────┘    └──────────────────────────────────┘   │
│                                  │                           │
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
KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name kafka-factory --wait 90s
```

Kind runs each Kubernetes node as a Podman container — no VMs needed.

### 2. Install the Strimzi operator (once)

```bash
make setup
# or directly:
bash scripts/install-strimzi.sh
```

This installs the Strimzi CRDs and operator into the `kafka-factory` namespace. Wait ~60s for it to become ready.

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
| 📊 Kafka UI | http://localhost:8080 |
| 📈 Prometheus | http://localhost:9090 |

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
│   │   └── kafka-topics.yaml       # 6 factory topics
│   ├── monitoring/
│   │   ├── prometheus-rbac.yaml
│   │   ├── prometheus-config.yaml
│   │   └── prometheus.yaml
│   ├── kafka-ui/
│   │   └── kafka-ui.yaml
│   └── factory-app/
│       ├── deployment-producer.yaml
│       ├── deployment-dashboard.yaml
│       └── service-dashboard.yaml
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
```

---

## Useful Commands

```bash
# Watch all pods
kubectl get pods -n kafka-factory -w

# All resources
make status

# Stream logs
make logs

# Tear down
make clean
```

---

## Prometheus Metrics

Strimzi exposes JMX metrics on port **9404** of each broker pod via the Prometheus JMX exporter. Prometheus scrapes these automatically using Kubernetes pod discovery with `prometheus.io/scrape: "true"` annotations.

Key metrics available:
- `kafka_server_brokertopicmetrics_messagesinpersec_total` — messages/sec per topic
- `kafka_server_replicamanager_underreplicatedpartitions` — replication health
- `kafka_network_requestmetrics_requestspersec_total` — request throughput
- `kafka_controller_*` — KRaft controller metrics

---

## Notes

- Kafka version: **3.9.0** (KRaft, no Zookeeper)
- Strimzi version: **0.45.0**
- All services are `ClusterIP` — access via Skaffold port-forward
- Storage: `persistent-claim` with `deleteClaim: true` (deleted with namespace)
- The producer generates ~1 event every 300ms–2.5s across all topics
- Skaffold builds images using the **Podman socket** via `DOCKER_HOST` — no Docker daemon needed
- The Makefile auto-detects the Podman socket path via `podman info`
