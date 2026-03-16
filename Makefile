.PHONY: setup dev run clean logs cluster

NAMESPACE        := kafka-factory
STRIMZI_VERSION  ?= 0.45.0
CLUSTER_NAME     ?= kafka-factory

# Resolve the Podman socket path (works on macOS podman machine and Linux rootless)
PODMAN_SOCK      ?= $(shell podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null)
export DOCKER_HOST := unix://$(PODMAN_SOCK)

## Create a local Kind cluster using Podman as the container provider
cluster:
	@echo "==> Creating Kind cluster '$(CLUSTER_NAME)' with Podman..."
	KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name $(CLUSTER_NAME) --wait 90s
	@echo "==> Cluster ready. Current context: $$(kubectl config current-context)"

## Bootstrap: install Strimzi operator (run once before skaffold)
setup:
	@echo "==> Using Podman socket: $(PODMAN_SOCK)"
	@echo "==> Installing Strimzi operator v$(STRIMZI_VERSION)..."
	bash scripts/install-strimzi.sh

## Start dev mode (hot-reload on file changes)
dev: setup
	skaffold dev --port-forward

## One-shot deploy (no file watching)
run: setup
	skaffold run --port-forward

## Tear down everything
clean:
	skaffold delete || true
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

## Stream logs from all factory-app pods
logs:
	kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/part-of=kafka-factory-demo --follow --max-log-requests=10

## Port-forward manually (if not using skaffold port-forward)
forward:
	kubectl port-forward -n $(NAMESPACE) svc/factory-dashboard 3000:3000 &
	kubectl port-forward -n $(NAMESPACE) svc/factory-dashboard 3001:3001 &
	kubectl port-forward -n $(NAMESPACE) svc/kafka-ui 8080:8080 &
	kubectl port-forward -n $(NAMESPACE) svc/prometheus 9090:9090 &
	@echo "Forwarding: dashboard→3000/3001, kafka-ui→8080, prometheus→9090"

## Show all pods in the kafka-factory namespace
status:
	kubectl get pods,svc,kafkatopic -n $(NAMESPACE)
