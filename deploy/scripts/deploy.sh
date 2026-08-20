#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# deploy.sh — Deploy CI/CD Observability Flink job
#
# Usage:
#   ./scripts/deploy.sh docker       # Run full stack in Docker
#   ./scripts/deploy.sh build        # Build Docker image only
#   ./scripts/deploy.sh test         # Run JUnit tests
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

IMAGE_NAME="cicd-flink-analytics"
IMAGE_TAG="${IMAGE_TAG:-latest}"
REGISTRY="${REGISTRY:-}"   # Set to your registry, e.g. docker.io/youruser

print_header() { echo; echo "══════════════════════════════════════"; echo "  $1"; echo "══════════════════════════════════════"; }
print_step()   { echo "  ▶  $1"; }
print_ok()     { echo "  ✅  $1"; }
print_err()    { echo "  ❌  $1"; exit 1; }

# ── Build ──────────────────────────────────────────────────────────────────────
build_image() {
    print_header "Building Docker image"
    print_step "Running Maven tests..."
    mvn test -q || print_err "Tests failed — fix before building"
    print_ok "Tests passed"

    print_step "Building fat JAR..."
    mvn package -DskipTests -q
    print_ok "JAR built: target/cicd-flink-analytics-1.0.0.jar"

    print_step "Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
    docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" -f Dockerfile .

    if [[ -n "${REGISTRY}" ]]; then
        FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
        docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${FULL_IMAGE}"
        print_step "Pushing to registry: ${FULL_IMAGE}"
        docker push "${FULL_IMAGE}"
        print_ok "Pushed: ${FULL_IMAGE}"
    fi
    print_ok "Docker image ready: ${IMAGE_NAME}:${IMAGE_TAG}"
}

# ── Docker deployment ──────────────────────────────────────────────────────────
deploy_docker() {
    print_header "Deploying to Docker"

    print_step "Building image..."
    build_image

    print_step "Starting all services..."
    docker compose up -d

    print_step "Waiting for Kafka to be healthy..."
    timeout 90 bash -c 'until docker inspect kafka --format="{{.State.Health.Status}}" 2>/dev/null | grep -q healthy; do sleep 3; done'
    print_ok "Kafka healthy"

    print_step "Waiting for Postgres to be healthy..."
    timeout 60 bash -c 'until docker inspect postgres --format="{{.State.Health.Status}}" 2>/dev/null | grep -q healthy; do sleep 3; done'
    print_ok "Postgres healthy"

    print_step "Waiting for Flink JobManager to start (may take 60s)..."
    timeout 120 bash -c 'until curl -sf http://localhost:8082/overview > /dev/null 2>&1; do sleep 5; done'
    print_ok "Flink JobManager running"

    print_step "Checking Flink job is running..."
    JOB_COUNT=$(curl -sf http://localhost:8082/jobs | python3 -c "import sys,json; d=json.load(sys.stdin); print(len([j for j in d.get('jobs',[]) if j.get('status')=='RUNNING']))" 2>/dev/null || echo "0")

    if [[ "$JOB_COUNT" -ge 1 ]]; then
        print_ok "Flink job is RUNNING ($JOB_COUNT job(s) active)"
    else
        echo "  ⚠️  No running Flink jobs yet — check logs:"
        echo "     docker compose logs flink-jm --tail=50"
    fi

    print_header "Stack is up!"
    echo "  Kafka UI:       http://localhost:8081"
    echo "  Flink Web UI:   http://localhost:8082"
    echo "  Grafana:        http://localhost:3000  (admin/admin)"
    echo "  Postgres:       localhost:5432  (flink/flink_secret)"
    echo
    echo "  Send a test event:"
    echo "  echo '{\"event\":{\"event_id\":\"t1\",\"pipeline_id\":\"bp\",\"service_name\":\"bloodpressure\",\"event_type\":\"BUILD_STARTED\",\"status\":\"SUCCESS\",\"event_timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}' | docker exec -i kcat kcat -P -b kafka:29092 -t cicd-events -k \"bp:BUILD_STARTED\""
}

# ── Tests ──────────────────────────────────────────────────────────────────────
run_tests() {
    print_header "Running JUnit tests"
    mvn test
    print_ok "All tests passed"
}

# ── Main ───────────────────────────────────────────────────────────────────────
case "${1:-help}" in
    docker) deploy_docker ;;
    build)  build_image   ;;
    test)   run_tests     ;;
    *)
        echo "Usage: $0 {docker|build|test}"
        echo
        echo "  docker  Build image and deploy full stack via docker compose"
        echo "  build   Build Docker image only (optionally push if REGISTRY is set)"
        echo "  test    Run JUnit tests"
        echo
        echo "Environment variables:"
        echo "  REGISTRY   Docker registry prefix, e.g. docker.io/youruser"
        echo "  IMAGE_TAG  Image tag (default: latest)"
        ;;
esac
