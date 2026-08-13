# AWS deployment — 3 EC2 instances

The single-host `docker-compose.yml` at the repo root is split into three
independent stacks, one per EC2 instance:

| Folder | EC2 # | Services | Talks to |
|---|---|---|---|
| `jenkins/` | 1 | jenkins, kcat | data-layer:9092 (outbound only) |
| `data-layer/` | 2 | kafka, kafka-ui, postgres, grafana | nothing outbound — everything else connects *to* it |
| `flink-job/` | 3 | flink-jm, flink-tm-1 | data-layer:9092, :5432, :3000 (outbound only) |

Each folder is a normal docker-compose project — `cd` into it and run
`docker compose up -d` on that instance. `data-layer/` and `flink-job/`
expect to run from inside a full checkout of this repo (they mount/build
from files elsewhere in the tree via relative paths); `jenkins/` is fully
self-contained and can be copied on its own if you prefer.

## Why three separate compose files, not one with profiles

Docker's bridge networks don't span hosts. Splitting means each instance
only runs what it needs, and the boundary between instances is explicit:
real TCP over each instance's IP + published port, governed by AWS security
groups — not container DNS names, which only resolve within one host's
docker network.

## Setup order

1. **data-layer** first — everything else depends on it being reachable.
   - `cd deploy/AWS/data-layer && cp .env.example .env`
   - Set `DATA_LAYER_HOST` to this instance's own private IP (same VPC) or
     public/Elastic IP (cross-VPC).
   - `docker compose up -d`
   - Confirm Kafka UI (`:8081`) and Grafana (`:3000`) load, then check
     Grafana → Postgres data source ("Test") the moment `flink-job` has
     written its first row.

2. **flink-job** — builds the Flink image from the repo's `Dockerfile`.
   - `cd deploy/AWS/flink-job && cp .env.example .env`
   - Set `DATA_LAYER_HOST` to the **same** value you set in step 1 (this is
     the data-layer instance's address, not this instance's own).
   - `docker compose up -d --build`
   - Check the Flink Web UI (`:8082`) — the job should be RUNNING and
     consuming from `cicd-events`.

3. **jenkins** — independent of the above two; start whenever.
   - `cd deploy/AWS/jenkins && cp .env.example .env`
   - Set `DATA_LAYER_HOST` to the data-layer instance's address (same as
     step 2).
   - `docker compose up -d`
   - Jenkins pipeline stages push events via
     `docker exec kcat kcat -P -b $KCAT_BROKERS -t cicd-events` (or
     `kafka-console-producer`), matching the JSON shape
     `CicdEventDeserializer` expects (see that class's javadoc).

## Security groups

Lock every rule down to the specific peer security group, never `0.0.0.0/0`:

| Instance | Port | Direction | From |
|---|---|---|---|
| data-layer | 9092 | inbound | flink-job SG, jenkins SG |
| data-layer | 5432 | inbound | flink-job SG |
| data-layer | 3000 | inbound | flink-job SG (annotations API), your IP (dashboard) |
| data-layer | 8081 | inbound | your IP only (Kafka UI) |
| flink-job | 8082 | inbound | your IP only (Flink Web UI) |
| jenkins | 8080 | inbound | your IP only (Jenkins UI) |

flink-job and jenkins make no inbound-required calls to each other or to
your own machine beyond the UI ports above — everything else is outbound
from them to data-layer.

## Known local-dev carryovers worth hardening before this is more than a demo

- Grafana runs with anonymous Editor access enabled, and Postgres accepts
  password auth from any host that can reach port 5432 — both inherited
  from the original local docker-compose.yml (see the SECURITY NOTE in
  `data-layer/docker-compose.yml`). Fine behind tight security groups;
  not fine if you ever open these ports more broadly.
- Passwords (`admin`/`admin`, `flink`/`admin`) are placeholders, same as
  local dev — replace via each service's environment block before any
  real use.
