.PHONY: cluster setup dev clean delete-cluster

NAMESPACE    := kafka-factory
CLUSTER_NAME := kafka-factory
PODMAN_SOCK  ?= $(shell podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null)
export DOCKER_HOST := unix://$(PODMAN_SOCK)

## Create Kind cluster with Podman (run once)
cluster:
	KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name $(CLUSTER_NAME) --wait 90s

## Install Strimzi operator (run once per cluster)
setup:
	bash scripts/install-strimzi.sh

## Start dev mode with hot-reload
dev:
	skaffold dev --port-forward

## Tear down everything
clean:
	skaffold delete || true
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

## Delete the Kind cluster entirely
delete-cluster:
	kind delete cluster --name $(CLUSTER_NAME)