"""
CI/CD Load Test Event Generator
================================
Generates realistic pipeline lifecycle events for Kafka load testing.

Usage:
  # File mode — write sample to JSON for review
  python3 generate_load_test_events.py
  python3 generate_load_test_events.py --start-after "2026-07-30T05:24:51Z"

  # Kafka mode — produce directly to Kafka
  python3 generate_load_test_events.py --kafka
  python3 generate_load_test_events.py --kafka --start-after "2026-07-30T05:24:51Z"

Arguments:
  --start-after <ISO-8601>   Events start from this timestamp + 1s.
                             Use the timestamp of your last sentinel event
                             so new events are always ahead of the watermark.
                             Example: --start-after "2026-07-30T05:24:51Z"

  --kafka                    Produce to Kafka instead of writing to JSON file.

  --count <N>                Number of events to generate (default: 1000).
                             Example: --count 100000

  --gap <seconds>            Seconds between steps in a lifecycle (default: 5).

If --start-after is not given, now = datetime.now(UTC) is used as the anchor.

Timestamp behaviour (Option B):
  base_ts for lifecycle N  =  start_ts + N × AVG_LIFECYCLE_DURATION_SECONDS
  Steps within a lifecycle  =  base_ts + step_index × EVENT_GAP_SECONDS

  This simulates lifecycles that ran sequentially in real time,
  producing naturally spreading timestamps across a multi-day window
  so Flink tumbling windows actually close and emit results.
"""

import argparse
import json
import random

try:
    import orjson          # faster Rust-based serialiser — used in Kafka mode only
    _kafka_dumps = orjson.dumps
except ImportError:
    _kafka_dumps = lambda obj: json.dumps(obj).encode()  # fallback: same bytes output
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

# ════════════════════════════════════════════════════════════════════════════
# DEFAULTS (overridable via CLI)
# ════════════════════════════════════════════════════════════════════════════
DEFAULT_TOTAL_EVENTS             = 1_000
DEFAULT_EVENT_GAP_SECONDS        = 5
DEFAULT_OUTPUT_FILE              = "load_test_events_sample.json"
AVG_LIFECYCLE_DURATION_SECONDS   = 18   # weighted avg of 3-step (15s) and 4-step (20s)

# Kafka
KAFKA_BOOTSTRAP  = "localhost:9092"
KAFKA_TOPIC      = "cicd-events"
KAFKA_BATCH_SIZE = 10_000
KAFKA_LINGER_MS  = 50
KAFKA_COMPRESS   = "snappy"

# "notification-service", "order-service",
#     "shipping-service", "user-service", "catalogue-service", "analytics-service",
#     "reporting-service", "gateway-service", "search-service", "billing-service",
#     "fulfillment-service", "recommendation-service", "pricing-service",
#     "review-service", "loyalty-service", "messaging-service"

PIPELINE_IDS = [
    "checkout-service", "payment-service", "auth-service",
    "inventory-service"
]

BRANCHES = [
    "main", "develop",
    "feature/checkout-v2", "feature/payment-retry",
    "hotfix/auth-token", "hotfix/deploy-fix",
    "release/v2.1.0", "release/v2.2.0",
]

LIFECYCLE_PATTERNS = {
    "pattern_1_deploy_success": [
        ("BUILD_STARTED",  "SUCCESS", "BUILD"),
        ("TEST_STARTED",   "SUCCESS", "TEST"),
        ("DEPLOY_SUCCESS", "SUCCESS", "DEPLOY"),
    ],
    "pattern_2_build_failed": [
        ("BUILD_STARTED", "SUCCESS", "BUILD"),
        ("TEST_STARTED",  "SUCCESS", "TEST"),
        ("BUILD_FAILED",  "FAILURE", "BUILD"),
    ],
    "pattern_3_full_success": [
        ("BUILD_STARTED",  "SUCCESS", "BUILD"),
        ("TEST_STARTED",   "SUCCESS", "TEST"),
        ("BUILD_SUCCESS",  "SUCCESS", "BUILD"),
        ("DEPLOY_SUCCESS", "SUCCESS", "DEPLOY"),
    ],
    "pattern_4_deploy_failed": [
        ("BUILD_STARTED",  "SUCCESS", "BUILD"),
        ("TEST_STARTED",   "SUCCESS", "TEST"),
        ("BUILD_SUCCESS",  "SUCCESS", "BUILD"),
        ("DEPLOY_FAILED",  "FAILURE", "DEPLOY"),
    ],
}

PATTERN_NAMES   = list(LIFECYCLE_PATTERNS.keys())
PATTERN_WEIGHTS = [25, 25, 30, 20]


# ════════════════════════════════════════════════════════════════════════════
# ARGUMENT PARSING
# ════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="CI/CD load test event generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # First run — use current time
  python3 generate_load_test_events.py

  # Second run — continue from sentinel timestamp so events are never late
  python3 generate_load_test_events.py --start-after "2026-07-30T05:24:51Z"

  # Kafka load test from a specific start point
  python3 generate_load_test_events.py --kafka --count 100000 --start-after "2026-07-30T05:24:51Z"
        """
    )
    parser.add_argument(
        "--start-after",
        type=str,
        default=None,
        metavar="TIMESTAMP",
        help=(
            "ISO-8601 timestamp to start events from (exclusive). "
            "First event will be at this timestamp + 1 second. "
            "Use the timestamp of your last sentinel event. "
            "If not given, current UTC time is used."
        ),
    )
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="Produce events to Kafka instead of writing to a JSON file.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_TOTAL_EVENTS,
        metavar="N",
        help=f"Number of events to generate (default: {DEFAULT_TOTAL_EVENTS}).",
    )
    parser.add_argument(
        "--gap",
        type=int,
        default=DEFAULT_EVENT_GAP_SECONDS,
        metavar="SECONDS",
        help=f"Seconds between steps within a lifecycle (default: {DEFAULT_EVENT_GAP_SECONDS}).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_FILE,
        metavar="FILE",
        help=f"Output JSON file path (default: {DEFAULT_OUTPUT_FILE}).",
    )
    return parser.parse_args()


def resolve_start_ts(start_after_arg):
    """
    Resolves the anchor timestamp for event generation.

    If --start-after is given:
        Parse it and add 1 second so the first event is strictly
        after the sentinel timestamp.
        This guarantees new events are always ahead of the watermark
        established by the sentinel.

    If --start-after is not given (or is '0'):
        Use datetime.now(UTC) — standard first run behaviour.
    """
    if start_after_arg and start_after_arg.strip() not in ("0", ""):
        try:
            # Handle both Z suffix and +00:00 offset
            ts_str  = start_after_arg.strip().replace("Z", "+00:00")
            parsed  = datetime.fromisoformat(ts_str)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            start_ts = parsed + timedelta(seconds=1)
            print(f"  --start-after  : {start_after_arg}")
            print(f"  Anchor ts      : {start_ts.strftime('%Y-%m-%dT%H:%M:%SZ')}  "
                  f"(sentinel + 1s)")
            return start_ts
        except ValueError as e:
            print(f"ERROR: Could not parse --start-after value '{start_after_arg}': {e}")
            print("       Expected format: 2026-07-30T05:24:51Z")
            sys.exit(1)
    else:
        start_ts = datetime.now(timezone.utc)
        print(f"  --start-after  : not given — using current UTC time")
        print(f"  Anchor ts      : {start_ts.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        return start_ts


# ════════════════════════════════════════════════════════════════════════════
# CORE GENERATOR
# ════════════════════════════════════════════════════════════════════════════

def generate_lifecycle(pipeline_id, build_num, base_ts, gap_seconds):
    """
    Generates one complete lifecycle starting at base_ts.
    Steps advance by gap_seconds each.
    """
    pattern_name = random.choices(PATTERN_NAMES, weights=PATTERN_WEIGHTS, k=1)[0]
    pattern      = LIFECYCLE_PATTERNS[pattern_name]
    commit_sha   = uuid.uuid4().hex[:12]
    branch       = random.choice(BRANCHES)
    service_name = pipeline_id.replace("-service", "").replace("-", "_")
    events       = []

    for step_idx, (event_type, status, stage) in enumerate(pattern):
        ts = base_ts + timedelta(seconds=step_idx * gap_seconds)
        events.append({
            "event": {
                "event_id":        f"evt-{pipeline_id[:3]}-{build_num:07d}-{step_idx:02d}",
                "pipeline_id":     pipeline_id,
                "repository_id":   pipeline_id,
                "analysis_name":   f"{pipeline_id}-analysis",
                "analysis_type":   "CI",
                "service_name":    service_name,
                "branch":          branch,
                "commit_sha":      commit_sha,
                "stage":           stage,
                "event_type":      event_type,
                "status":          status,
                "event_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            "_meta": {
                "pattern":    pattern_name,
                "build_num":  build_num,
                "step":       step_idx + 1,
                "step_total": len(pattern),
            }
        })

    return events


def event_generator(start_ts, total_events, gap_seconds):
    """
    Yields events with Option B timestamps.

    Lifecycle N starts at:  start_ts + N × AVG_LIFECYCLE_DURATION_SECONDS

    start_ts is either:
      - sentinel_ts + 1s   (if --start-after was given)
      - datetime.now()     (if not given)

    In both cases, generated events are always strictly ahead of the
    last known watermark, so no event arrives as late to Flink.
    """
    build_num = 1
    emitted   = 0

    while emitted < total_events:
        pipeline_id = random.choice(PIPELINE_IDS)
        base_ts     = start_ts + timedelta(
                          seconds=build_num * AVG_LIFECYCLE_DURATION_SECONDS)
        lifecycle   = generate_lifecycle(pipeline_id, build_num, base_ts, gap_seconds)

        for event in lifecycle:
            if emitted >= total_events:
                return
            yield event
            emitted += 1

        build_num += 1


# ════════════════════════════════════════════════════════════════════════════
# MODE 1 — Write to JSON file
# ════════════════════════════════════════════════════════════════════════════

def write_to_file(start_ts, total_events, gap_seconds, output_file):
    print(f"\n  Generating {total_events:,} events → {output_file}")
    t0     = time.perf_counter()
    events = list(event_generator(start_ts, total_events, gap_seconds))

    with open(output_file, "w") as f:
        json.dump(events, f, indent=2)

    elapsed = time.perf_counter() - t0

    pattern_counts    = {}
    event_type_counts = {}
    for e in events:
        p  = e["_meta"]["pattern"]
        et = e["event"]["event_type"]
        pattern_counts[p]     = pattern_counts.get(p, 0) + 1
        event_type_counts[et] = event_type_counts.get(et, 0) + 1

    print(f"\n{'='*64}")
    print(f"  Output file    : {output_file}")
    print(f"  Total events   : {len(events):,}")
    print(f"  Generated in   : {elapsed:.3f}s  ({len(events)/elapsed:,.0f} events/sec)")
    print(f"  First ts       : {events[0]['event']['event_timestamp']}")
    print(f"  Last ts        : {events[-1]['event']['event_timestamp']}")
    print(f"\n  Lifecycle start timestamps (first 6 builds):")
    seen = []
    for e in events:
        bn = e["_meta"]["build_num"]
        if bn not in seen and e["_meta"]["step"] == 1:
            seen.append(bn)
            print(f"    build {bn:<5}  {e['_meta']['pattern']:<35}  "
                  f"{e['event']['event_timestamp']}")
        if len(seen) >= 6:
            break
    print(f"\n  Pattern distribution:")
    for p, c in sorted(pattern_counts.items()):
        print(f"    {p:<45} {c:>5}  ({c/len(events)*100:.1f}%)")
    print(f"\n  Event type distribution:")
    for et, c in sorted(event_type_counts.items(), key=lambda x: -x[1]):
        print(f"    {et:<30} {c:>5}  ({c/len(events)*100:.1f}%)")
    print(f"{'='*64}")
    print(f"\nNext run — pass the last ts as sentinel anchor:")
    print(f"  python3 generate_load_test_events.py "
          f"--start-after \"{events[-1]['event']['event_timestamp']}\"")


# ════════════════════════════════════════════════════════════════════════════
# MODE 2 — Produce to Kafka
# ════════════════════════════════════════════════════════════════════════════

def produce_to_kafka(start_ts, total_events, gap_seconds):
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("confluent-kafka not installed.  Run: pip install confluent-kafka")
        sys.exit(1)

    conf = {
        "bootstrap.servers":            KAFKA_BOOTSTRAP,
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes":   512_000,
        "batch.num.messages":           KAFKA_BATCH_SIZE,
        "linger.ms":                    KAFKA_LINGER_MS,
        "compression.type":             KAFKA_COMPRESS,
        "acks":                         "1",
    }

    producer  = Producer(conf)
    delivered = [0]
    errors    = [0]
    last_ts   = [None]

    def delivery_report(err, msg):
        if err:
            errors[0] += 1
        else:
            delivered[0] += 1

    print(f"\n  Producing {total_events:,} events → {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
    t0 = time.perf_counter()

    for i, event in enumerate(event_generator(start_ts, total_events, gap_seconds), 1):
        last_ts[0] = event["event"]["event_timestamp"]
        producer.produce(
            topic    = KAFKA_TOPIC,
            key      = event["event"]["pipeline_id"],
            value    = _kafka_dumps(event),   # orjson if available, else json
            callback = delivery_report,
        )
        if i == 1:
            # Captured immediately after the first producer.produce() call
            # returns, not before the loop starts — avoids counting
            # event_generator()'s per-event setup (random.choice,
            # generate_lifecycle, timestamp math) against e2e latency.
            first_event_pushed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"  First event pushed to Kafka at (wall-clock UTC): {first_event_pushed_at}")
            print(f"  First event id: {event['event']['event_id']}")
            print(f"  Compare against the 'Job Completed At' dashboard panel for e2e latency.")
        if i % KAFKA_BATCH_SIZE == 0:
            producer.poll(0)
            elapsed = time.perf_counter() - t0
            print(f"  {i:>10,} / {total_events:,}  "
                  f"{i/elapsed:>10,.0f} events/sec  "
                  f"delivered={delivered[0]:,}  errors={errors[0]}")

    producer.flush()
    elapsed = time.perf_counter() - t0

    print(f"\n{'='*64}")
    print(f"  Total sent   : {total_events:,}")
    print(f"  Delivered    : {delivered[0]:,}")
    print(f"  Errors       : {errors[0]}")
    print(f"  Elapsed      : {elapsed:.2f}s")
    print(f"  Throughput   : {total_events / elapsed:,.0f} events/sec")
    print(f"  Last ts      : {last_ts[0]}")
    print(f"{'='*64}")
    print(f"\nNext run — pass last ts as sentinel anchor:")
    print(f"  python3 generate_load_test_events.py --kafka "
          f"--start-after \"{last_ts[0]}\"")


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = parse_args()

    print("\nCI/CD Load Test Event Generator")
    print("="*64)
    start_ts = resolve_start_ts(args.start_after)
    print(f"  Total events   : {args.count:,}")
    print(f"  Event gap      : {args.gap}s between lifecycle steps")

    if args.kafka:
        produce_to_kafka(start_ts, args.count, args.gap)
    else:
        write_to_file(start_ts, args.count, args.gap, args.output)
