# CI/CD Observability — Apache Flink Analytics

Real-time CI/CD pipeline observability using Apache Flink.
Consumes events from Kafka (emitted by Jenkins), computes four use cases,
and sinks results to Kafka, PostgreSQL, and Grafana.

---

## Project structure

```
cicd-observability-flink/
│
├── pom.xml                                  ← Maven build (fat JAR)
├── Dockerfile                               ← Single image: JM + TM roles
├── docker-compose.yml                       ← Full local stack
├── .gitignore
│
├── src/
│   ├── main/
│   │   ├── java/com/cicd/observability/
│   │   │   ├── config/
│   │   │   │   └── FlinkConfig.java         ← RocksDB, watermarks, Kafka sources/sinks
│   │   │   ├── deserializer/
│   │   │   │   └── CicdEventDeserializer.java ← Kafka JSON → CicdEvent
│   │   │   ├── model/
│   │   │   │   ├── CicdEvent.java           ← Jenkins event POJO
│   │   │   │   └── MetricResult.java        ← Output metric POJO
│   │   │   ├── router/
│   │   │   │   └── EventRouter.java         ← Routes events to correct use-case stream
│   │   │   ├── operators/
│   │   │   │   ├── dora/
│   │   │   │   │   ├── DeploymentFrequencyOperator.java  ← DORA #1
│   │   │   │   │   └── DoraOperators.java               ← DORA #2 #3 #4
│   │   │   │   ├── health/
│   │   │   │   │   └── PipelineHealthOperator.java      ← Health score
│   │   │   │   ├── late/
│   │   │   │   │   └── LateEventOperator.java           ← Late event + side output
│   │   │   │   └── cep/
│   │   │   │       └── FailurePatternOperator.java      ← Flink CEP pattern detection
│   │   │   ├── sink/
│   │   │   │   ├── PostgresMetricSink.java  ← MetricResult → PostgreSQL
│   │   │   │   ├── PostgresStringSink.java  ← CEP alerts → PostgreSQL
│   │   │   │   └── GrafanaSink.java         ← Annotations → Grafana HTTP API
│   │   │   └── jobs/
│   │   │       └── PipelineObservabilityJob.java  ← main() — wires everything
│   │   └── resources/
│   │       ├── init.sql                     ← PostgreSQL schema (run once)
│   │       └── log4j2.properties
│   └── test/
│       └── java/com/cicd/observability/
│           ├── dora/DoraOperatorsTest.java
│           ├── health/PipelineHealthOperatorTest.java
│           ├── late/LateEventOperatorTest.java
│           └── cep/FailurePatternOperatorTest.java
│
└── deploy/
    ├── docker/
    │   ├── flink-conf.yaml                  ← Flink config baked into image
    │   └── grafana-datasource.yaml          ← Grafana auto-provisioning
    └── scripts/
        └── deploy.sh                        ← One-command deploy for Docker
```

---

## Four use cases

| Use case | Trigger (EventRouter) | Flink feature | Output topic |
|---|---|---|---|
| DORA metrics (DF, LT, CFR, MTTR) | `BUILD_*`, `DEPLOY_*` events | `TumblingEventTimeWindows`, `KeyedProcessFunction`, `MapState` | `pipeline-metrics` |
| Pipeline health score | All stage events (`BUILD_*`, `TEST_*`, `SONARQUBE_*`, `PACKAGE_*`) | `SlidingEventTimeWindows` | `pipeline-health` |
| Late event processing | Events older than 30s at arrival | `allowedLateness`, `sideOutputLateData`, `OutputTag` | `pipeline-metrics`, `late-events` |
| Failure pattern detection | `DEPENDENCY_UPDATED`, `UNIT_TEST_FAILED`, `RETRY_TRIGGERED`, `INTEGRATION_TEST_FAILED`, `ROLLBACK_TRIGGERED` | Flink CEP `Pattern`, `CEP.pattern()`, `PatternSelectFunction`, `PatternTimeoutFunction` | `failure-pattern-alerts`, `pattern-timeouts` |

All results are sunk to **Kafka** (streaming) + **PostgreSQL** (storage) + **Grafana** (annotations).

---

## Quick start

### Run locally with Docker Compose

```bash
# 1. Build image and start full stack
./deploy/scripts/deploy.sh docker

# Or manually:
docker build -t cicd-flink-analytics:latest .
docker compose up -d

# 2. Verify Flink job is running
curl http://localhost:8082/jobs
# Open Flink Web UI: http://localhost:8082

# 3. Send a test event (triggers DORA + Health use cases)
echo '{"event":{"event_id":"t1","pipeline_id":"bp-svc","service_name":"bloodpressure","event_type":"BUILD_STARTED","status":"SUCCESS","event_timestamp":"2026-07-05T10:00:00Z"}}' \
  | docker exec -i kcat kcat -P -b kafka:29092 -t cicd-events -k "bp:BUILD_STARTED"

# 4. Send a CEP pattern event sequence (triggers failure pattern detection)
for TYPE in DEPENDENCY_UPDATED UNIT_TEST_FAILED RETRY_TRIGGERED INTEGRATION_TEST_FAILED ROLLBACK_TRIGGERED; do
  echo "{\"event\":{\"event_id\":\"cep-$RANDOM\",\"pipeline_id\":\"bp-svc\",\"service_name\":\"bloodpressure\",\"event_type\":\"$TYPE\",\"status\":\"FAILURE\",\"event_timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
    | docker exec -i kcat kcat -P -b kafka:29092 -t cicd-events -k "bp:$TYPE"
  sleep 5
done

# 5. Check results
docker exec kcat kcat -C -b kafka:29092 -t pipeline-metrics -e
docker exec kcat kcat -C -b kafka:29092 -t failure-pattern-alerts -e
```

**URLs:**

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8081 |
| Flink Web UI | http://localhost:8082 |
| Grafana | http://localhost:3000 (admin/admin) |
| PostgreSQL | localhost:5432 (flink/flink_secret) |

---

## Build and test

```bash
# Run tests
mvn test

# Build fat JAR
mvn clean package -DskipTests

# Build Docker image
docker build -t cicd-flink-analytics:1.0.0 .
```

---

## PostgreSQL schema

Schema is auto-applied on first start via `init.sql` mounted as Docker init script.

Key tables:

```sql
-- DORA metrics, health scores, late event metrics
SELECT metric_type, pipeline_id, value, performance_band, inserted_at
FROM cicd_metrics ORDER BY inserted_at DESC LIMIT 20;

-- CEP failure alerts, pattern timeouts, late event audit
SELECT alert_type, payload->>'pipeline_id', inserted_at
FROM cicd_alerts ORDER BY inserted_at DESC LIMIT 20;
```

---

## Grafana dashboards

Grafana auto-provisions PostgreSQL as a data source on startup.

Example Grafana panel queries:

```sql
-- Deployment Frequency over time
SELECT to_timestamp(window_end_ms/1000) AS time, pipeline_id, value AS deploys_per_day
FROM cicd_metrics
WHERE metric_type = 'DEPLOYMENT_FREQUENCY' AND $__timeFilter(to_timestamp(window_end_ms/1000))
ORDER BY time;

-- Pipeline health score
SELECT to_timestamp(window_end_ms/1000) AS time, pipeline_id, value AS health_score, performance_band
FROM cicd_metrics
WHERE metric_type = 'PIPELINE_HEALTH_SCORE' AND $__timeFilter(to_timestamp(window_end_ms/1000))
ORDER BY time;

-- Failure pattern alerts
SELECT inserted_at AS time, payload->>'pipeline_id' AS pipeline, payload->>'cascade_duration_minutes' AS duration_min
FROM cicd_alerts
WHERE alert_type = 'FAILURE_PATTERN' AND $__timeFilter(inserted_at)
ORDER BY inserted_at;
```

---

## How the single image runs both JobManager and TaskManager

```
docker-compose.yml
│
├── flink-jm:  command: ["standalone-job", "--job-classname", "...PipelineObservabilityJob"]
│               → Loads JAR from /opt/flink/usrlib/
│               → Calls PipelineObservabilityJob.main()
│               → Builds streaming DAG (EventRouter + 4 use cases)
│               → Schedules tasks to TaskManagers
│               → Manages checkpoints (RocksDB → /tmp/flink-checkpoints)
│
└── flink-tm-*: command: ["taskmanager"]
                → Registers with JobManager on port 6123
                → Provides task slots
                → Executes assigned operator tasks:
                    TM-1: EventRouter, DORA operators
                    TM-2: HealthOperator, CEP NFA, LateEventOperator
                → Each operator only processes events routed to it by EventRouter
```

**RocksDB state** is used by:
- `LeadTimeProcessFn` — `MapState<commitSha, startMs>` per pipeline
- `MttrProcessFn` — `ValueState<failureStartMs>` per pipeline
- CEP NFA — Flink manages NFA state internally using keyed state
- Checkpoints written every 10s to `/tmp/flink-checkpoints`
