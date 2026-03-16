#!/usr/bin/env bash
# Installs the Strimzi operator into the kafka-factory namespace.
# Run this once before using `skaffold dev` or `skaffold run`.

set -euo pipefail

STRIMZI_VERSION="${STRIMZI_VERSION:-0.45.0}"
NAMESPACE="kafka-factory"

echo "==> Creating namespace ${NAMESPACE} (if not exists)..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Installing Strimzi operator v${STRIMZI_VERSION} into namespace ${NAMESPACE}..."
kubectl apply -f "https://strimzi.io/install/latest?namespace=${NAMESPACE}" -n "${NAMESPACE}"

echo "==> Waiting for Strimzi operator to be ready..."
kubectl rollout status deployment/strimzi-cluster-operator -n "${NAMESPACE}" --timeout=180s

echo "==> Strimzi operator is ready!"
