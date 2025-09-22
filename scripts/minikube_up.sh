#!/usr/bin/env bash
set -euo pipefail

PROFILE=${1:-tokenize-poc}
NAMESPACE=${2:-tokenize-poc}
IMAGE=${3:-tokenize-poc-action:latest}
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

if ! minikube -p "$PROFILE" status >/dev/null 2>&1; then
  echo "Starting minikube profile $PROFILE"
  minikube start -p "$PROFILE" --cpus=4 --memory=8192 --driver=docker || minikube start -p "$PROFILE"
fi

kubectl config use-context "minikube" >/dev/null 2>&1 || true
kubectl config use-context "$PROFILE"

helm repo add bitnami https://charts.bitnami.com/bitnami >/dev/null 2>&1 || true
helm repo add acryldata https://helm.acryl.io >/dev/null 2>&1 || true
helm repo update >/dev/null

kubectl apply -f "$ROOT_DIR/k8s/namespace.yaml"

if [[ ! -f "$ROOT_DIR/k8s/secrets.env" ]]; then
  cp "$ROOT_DIR/k8s/secrets.example.env" "$ROOT_DIR/k8s/secrets.env"
  echo "Created k8s/secrets.env from example. Please populate credentials and rerun."
  exit 1
fi

set -a
source "$ROOT_DIR/k8s/secrets.env"
set +a

export NAMESPACE="$NAMESPACE"
export PG_CONN_STR_B64="$(printf '%s' "${PG_CONN_STR:-}" | base64 | tr -d '\n')"
export DBX_JDBC_URL_B64="$(printf '%s' "${DBX_JDBC_URL:-}" | base64 | tr -d '\n')"
export TOKEN_SDK_MODE_B64="$(printf '%s' "${TOKEN_SDK_MODE:-dummy}" | base64 | tr -d '\n')"

envsubst < "$ROOT_DIR/k8s/secrets.yaml.tpl" > "$ROOT_DIR/k8s/secrets.yaml"

kubectl apply -f "$ROOT_DIR/k8s/secrets.yaml"
kubectl apply -f "$ROOT_DIR/k8s/rbac.yaml"

helm upgrade --install postgresql bitnami/postgresql \
  --namespace "$NAMESPACE" \
  --values "$ROOT_DIR/k8s/postgres-values.yaml" \
  --wait

minikube -p "$PROFILE" image build -t "$IMAGE" -f "$ROOT_DIR/docker/action.Dockerfile" "$ROOT_DIR"

kubectl apply -f "$ROOT_DIR/k8s/action-deployment.yaml"
kubectl apply -f "$ROOT_DIR/k8s/action-service.yaml"

"$ROOT_DIR/scripts/wait_for.sh" "$NAMESPACE" "deployment/tokenize-poc-action"
"$ROOT_DIR/scripts/seed_pg.sh" "$NAMESPACE"
"$ROOT_DIR/scripts/wait_for.sh" "$NAMESPACE" "deployment/tokenize-poc-action"
"$ROOT_DIR/scripts/wait_for.sh" "$NAMESPACE" "service/tokenize-poc-action"
