# ═══════════════════════════════════════════════════════════════════════════════
# CI/CD Observability — Flink Application Image
#
# This SINGLE image is used for BOTH JobManager AND TaskManager.
# The role is determined at runtime by the Docker/K8s command argument:
#   args: ["standalone-job"]  → JobManager (loads + submits the Flink job)
#   args: ["taskmanager"]     → TaskManager (executes operator tasks)
#
# The fat JAR is placed in /opt/flink/usrlib/ which Flink's Application Mode
# scans automatically — no separate job submission step needed.
# ═══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: Maven build ───────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Cache Maven dependencies separately from source (faster rebuilds)
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Build fat JAR
COPY src ./src
RUN mvn clean package -DskipTests -q

# ── Stage 2: Flink runtime ─────────────────────────────────────────────────────
FROM flink:1.20.5-java17

# The JAR goes into /opt/flink/usrlib — Flink Application Mode scans this
# directory automatically and submits the job when JobManager starts.
COPY --from=builder \
    /build/target/cicd-flink-analytics-1.0.0.jar \
    /opt/flink/usrlib/cicd-flink-analytics.jar

# PostgreSQL init SQL — useful for first-time DB setup from within the cluster
COPY src/main/resources/init.sql /opt/flink/init.sql

# Pre-create the checkpoint and RocksDB local-state directories owned by the
# flink user (uid 9999). Docker copies this ownership into the named volume
# mounted at the same path on first container start — without it, the volume
# is created root-owned (RUN executes as root at build time; the image
# switches to flink only via its entrypoint script at runtime) and the
# non-root flink user can't write checkpoint/RocksDB state to it.
RUN mkdir -p /tmp/flink-checkpoints /tmp/rocksdb && \
    chown -R flink:flink /tmp/flink-checkpoints /tmp/rocksdb

# Override Flink config with our job-specific settings
# These are defaults — all values can be overridden via env vars in Docker/K8s
COPY deploy/docker/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Environment variable defaults (overridden in docker-compose / K8s ConfigMap)
ENV KAFKA_BOOTSTRAP="kafka:29092" \
    KAFKA_TOPIC_INPUT="cicd-events" \
    FLINK_PARALLELISM="2" \
    PG_URL="jdbc:postgresql://postgres:5432/cicd_metrics" \
    PG_USER="flink" \
    PG_PASSWORD="admin" \
    GRAFANA_URL="http://grafana:3000" \
    GRAFANA_API_KEY="changeme"

# Health check — works for both JobManager (HTTP REST) and TaskManager (TCP)
# Docker will use this; K8s uses its own probes defined in manifests
HEALTHCHECK --interval=15s --timeout=10s --start-period=40s --retries=5 \
    CMD curl -f http://localhost:8081/overview 2>/dev/null \
        || nc -z localhost 6122 2>/dev/null \
        || exit 1

# No CMD/ENTRYPOINT override — the base Flink image handles this.
# Role is set by: args: ["standalone-job"] or args: ["taskmanager"]
