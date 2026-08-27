#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

docker build -t cicd-flink-analytics:latest "$REPO_ROOT"
docker save cicd-flink-analytics:latest -o /tmp/cicd-flink-analytics.tar
sudo k3s ctr images import /tmp/cicd-flink-analytics.tar

envsubst < "$SCRIPT_DIR/flink-deployment.yaml" | kubectl apply -f -
