#!/usr/bin/env bash
# Installs the Strimzi operator using Helm chart.
# The operator will watch all namespaces.
# Run this once before using `skaffold dev` or `skaffold run`.

set -euo pipefail

OPERATOR_NAMESPACE="strimzi-operator"
APP_NAMESPACE="kafka-car-factory"

echo "==> Checking if Helm is installed..."
if ! command -v helm &> /dev/null; then
    echo "❌ ERROR: Helm is not installed. Please install Helm first:"
    echo "   https://helm.sh/docs/intro/install/"
    exit 1
fi

echo "==> Adding Strimzi Helm repository..."
helm repo add strimzi https://strimzi.io/charts/
helm repo update

echo "==> Creating operator namespace ${OPERATOR_NAMESPACE}..."
kubectl create namespace "${OPERATOR_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Creating application namespace ${APP_NAMESPACE}..."
kubectl create namespace "${APP_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Installing Strimzi operator via Helm..."
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator \
  --namespace "${OPERATOR_NAMESPACE}" \
  --set watchAnyNamespace=true \
  --wait

echo "==> Strimzi operator is ready and watching all namespaces!"
