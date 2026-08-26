"""
CEP Deployment-Failure Event Generator
========================================
Pushes a small, hand-crafted batch of events (4-6) that exercise one of the
three Flink CEP deployment-failure patterns in FailurePatternOperator:

  rollback-cascade   DEPLOY_FAILED -> DEPLOY_FAILED -> ROLLBACK_STARTED
                      within 10 min. Two failures ending in an automatic
                      rollback. (default scenario)

  instability         DEPLOY_FAILED -> DEPLOY_STARTED -> DEPLOY_FAILED
                      within 10 min. A retry that fails again, no rollback.

  build-broken        BUILD_SUCCESS -> DEPLOY_FAILED -> DEPLOY_FAILED
                      within 15 min. The app builds fine but deployment
                      keeps failing — points at infra/config, not code.

Each scenario's full event list is exactly what's needed for a COMPLETE
match (plus one or two realistic context events, e.g. BUILD_STARTED /
DEPLOY_STARTED, that are harmless noise to the pattern). --count controls
how many of those events actually get sent — passing fewer than the full
list produces a deliberately PARTIAL match, which fires
PatternTimeoutFunction once the pattern's time window elapses.

Usage:
  # File mode - write sample to JSON for review
  python3 generate_cep_events.py
  python3 generate_cep_events.py --scenario instability

  # Kafka mode - produce directly to Kafka
  python3 generate_cep_events.py --kafka
  python3 generate_cep_events.py --kafka --scenario build-broken

Arguments:
  --scenario <name>          rollback-cascade (default) | instability | build-broken

  --start-after <ISO-8601>   First event starts at this timestamp + 1s.
                             Use the timestamp of your last sentinel event
                             so new events are always ahead of the watermark.

  --kafka                    Produce to Kafka instead of writing to JSON file.

  --count <N>                Number of events to emit, from the start of the
                             scenario's event list (default: all of them —
                             a complete match). Pass fewer for a partial
                             match / timeout demo.

  --gap <seconds>            Seconds between events (default: 90). Must stay
                             well under the scenario's CEP window (10 or 15 min).

  --pipeline-id <id>         Pipeline id to use (default: cycles through
                             checkout-service, payment-service, auth-service,
                             inventory-service, one per lifecycle).
"""

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone, timedelta

# ════════════════════════════════════════════════════════════════════════════
# DEFAULTS (overridable via CLI)
# ════════════════════════════════════════════════════════════════════════════
DEFAULT_EVENT_GAP_SECONDS = 90
DEFAULT_OUTPUT_FILE       = "cep_events_sample.json"

# Kafka
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "cicd-events"

PIPELINE_IDS = [
    "checkout-service", "payment-service", "auth-service",
    "inventory-service",
]

# ── Scenario definitions ───────────────────────────────────────────────────
# Each event list is what FailurePatternOperator's matching pattern expects,
# plus realistic leading context events (BUILD_STARTED/DEPLOY_STARTED) that
# are harmless noise to the NFA (.followedBy() uses relaxed contiguity).
#
# gap_seconds and window_minutes must satisfy:
#   (len(events) - 1) * gap_seconds < window_minutes * 60
SCENARIOS = {
    # DEPLOY_FAILED -> DEPLOY_FAILED -> ROLLBACK_STARTED, within 10 min
    "rollback-cascade": {
        "window_minutes": 10,
        "events": [
            ("BUILD_STARTED",    "SUCCESS", "BUILD"),
            ("BUILD_SUCCESS",    "SUCCESS", "BUILD"),
            ("DEPLOY_STARTED",   "SUCCESS", "DEPLOY"),
            ("DEPLOY_FAILED",    "FAILURE", "DEPLOY"),   # first_failure
            ("DEPLOY_FAILED",    "FAILURE", "DEPLOY"),   # second_failure
            ("ROLLBACK_STARTED", "FAILURE", "DEPLOY"),   # rollback
        ],
    },
    # DEPLOY_FAILED -> DEPLOY_STARTED -> DEPLOY_FAILED, within 10 min
    "instability": {
        "window_minutes": 10,
        "events": [
            ("DEPLOY_STARTED", "SUCCESS", "DEPLOY"),
            ("DEPLOY_FAILED",  "FAILURE", "DEPLOY"),   # first_failure
            ("DEPLOY_STARTED", "SUCCESS", "DEPLOY"),   # retry_deploy
            ("DEPLOY_FAILED",  "FAILURE", "DEPLOY"),   # second_failure
        ],
    },
    # BUILD_SUCCESS -> DEPLOY_FAILED -> DEPLOY_FAILED, within 15 min
    "build-broken": {
        "window_minutes": 15,
        "events": [
            ("BUILD_STARTED", "SUCCESS", "BUILD"),
            ("BUILD_SUCCESS", "SUCCESS", "BUILD"),      # build_ok
            ("DEPLOY_STARTED","SUCCESS", "DEPLOY"),
            ("DEPLOY_FAILED", "FAILURE", "DEPLOY"),     # first_failure
            ("DEPLOY_STARTED","SUCCESS", "DEPLOY"),
            ("DEPLOY_FAILED", "FAILURE", "DEPLOY"),     # second_failure
        ],
    },
}


# ════════════════════════════════════════════════════════════════════════════
# ARGUMENT PARSING
# ════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="CEP deployment-failure event generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Complete rollback-cascade match (default scenario)
  python3 generate_cep_events.py --kafka

  # Complete deployment-instability match
  python3 generate_cep_events.py --kafka --scenario instability

  # Build-broken match, but truncated -> timeout demo
  python3 generate_cep_events.py --kafka --scenario build-broken --count 4
        """
    )
    parser.add_argument(
        "--scenario",
        type=str,
        choices=list(SCENARIOS.keys()),
        default="rollback-cascade",
        help="Which CEP pattern to generate events for (default: rollback-cascade).",
    )
    parser.add_argument(
        "--start-after",
        type=str,
        default=None,
        metavar="TIMESTAMP",
        help=(
            "ISO-8601 timestamp to start events from (exclusive). "
            "First event will be at this timestamp + 1 second. "
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
        default=None,
        metavar="N",
        help="Number of events to emit from the scenario's list "
             "(default: all of them — a complete match). Pass fewer for a "
             "partial match / timeout demo.",
    )
    parser.add_argument(
        "--gap",
        type=int,
        default=DEFAULT_EVENT_GAP_SECONDS,
        metavar="SECONDS",
        help=f"Seconds between events (default: {DEFAULT_EVENT_GAP_SECONDS}).",
    )
    parser.add_argument(
        "--pipeline-id",
        type=str,
        default=None,
        metavar="ID",
        help="Pipeline id to use for every event (default: cycle through a "
             "small fixed list, one id per lifecycle).",
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

    If --start-after is given: parse it and add 1 second so the first
    event is strictly after the sentinel timestamp (keeps it ahead of
    the watermark). Otherwise uses datetime.now(UTC).
    """
    if start_after_arg and start_after_arg.strip() not in ("0", ""):
        try:
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

def generate_events(scenario_name, start_ts, count, gap_seconds, pipeline_id):
    scenario     = SCENARIOS[scenario_name]
    steps        = scenario["events"]
    steps_wanted = len(steps) if count is None else min(count, len(steps))
    commit_sha   = uuid.uuid4().hex[:12]
    service_name = pipeline_id.replace("-service", "").replace("-", "_")
    events       = []

    for step_idx, (event_type, status, stage) in enumerate(steps[:steps_wanted]):
        ts = start_ts + timedelta(seconds=step_idx * gap_seconds)
        events.append({
            "event": {
                "event_id":        f"evt-cep-{scenario_name[:2]}-{pipeline_id[:3]}-{step_idx:02d}",
                "pipeline_id":     pipeline_id,
                "repository_id":   pipeline_id,
                "analysis_name":   f"{pipeline_id}-analysis",
                "analysis_type":   "CI",
                "service_name":    service_name,
                "branch":          "main",
                "commit_sha":      commit_sha,
                "stage":           stage,
                "event_type":      event_type,
                "status":          status,
                "event_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            "_meta": {
                "scenario":   scenario_name,
                "step":       step_idx + 1,
                "step_total": len(steps),
            }
        })

    return events


# ════════════════════════════════════════════════════════════════════════════
# MODE 1 — Write to JSON file
# ════════════════════════════════════════════════════════════════════════════

def write_to_file(events, output_file):
    with open(output_file, "w") as f:
        json.dump(events, f, indent=2)

    print(f"\n{'='*64}")
    print(f"  Output file    : {output_file}")
    print(f"  Total events   : {len(events)}")
    _print_events(events)
    print(f"{'='*64}")
    print(f"\nNext run — pass the last ts as sentinel anchor:")
    print(f"  python3 generate_cep_events.py --start-after \"{events[-1]['event']['event_timestamp']}\"")


# ════════════════════════════════════════════════════════════════════════════
# MODE 2 — Produce to Kafka
# ════════════════════════════════════════════════════════════════════════════

def produce_to_kafka(events):
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("confluent-kafka not installed.  Run: pip install confluent-kafka")
        sys.exit(1)

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks":              "1",
    }

    producer  = Producer(conf)
    delivered = [0]
    errors    = [0]

    def delivery_report(err, msg):
        if err:
            errors[0] += 1
        else:
            delivered[0] += 1

    print(f"\n  Producing {len(events)} event(s) → {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")

    for event in events:
        ev = event["event"]
        producer.produce(
            topic    = KAFKA_TOPIC,
            key      = ev["pipeline_id"],
            value    = json.dumps(event).encode(),
            callback = delivery_report,
        )
        producer.poll(0)

    producer.flush()
    _print_events(events)

    print(f"\n{'='*64}")
    print(f"  Total sent   : {len(events)}")
    print(f"  Delivered    : {delivered[0]}")
    print(f"  Errors       : {errors[0]}")
    print(f"  Last ts      : {events[-1]['event']['event_timestamp']}")
    print(f"{'='*64}")
    print(f"\nNext run — pass last ts as sentinel anchor:")
    print(f"  python3 generate_cep_events.py --kafka --start-after \"{events[-1]['event']['event_timestamp']}\"")


# ════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _print_events(events):
    for e in events:
        m  = e["_meta"]
        ev = e["event"]
        complete = " (COMPLETE match)" if m["step"] == m["step_total"] else ""
        print(f"    [{m['scenario']}] step {m['step']}/{m['step_total']}  "
              f"{ev['event_type']:<18} {ev['pipeline_id']:<20} {ev['event_timestamp']}{complete}")


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = parse_args()

    scenario = SCENARIOS[args.scenario]
    window_seconds = (len(scenario["events"]) - 1) * args.gap
    if window_seconds >= scenario["window_minutes"] * 60:
        print(f"ERROR: --gap {args.gap}s over {len(scenario['events'])} events spans "
              f"{window_seconds}s, which does not fit inside the {args.scenario} "
              f"pattern's {scenario['window_minutes']}-minute CEP window. Lower --gap.")
        sys.exit(1)

    print("\nCEP Deployment-Failure Event Generator")
    print("="*64)
    print(f"  Scenario       : {args.scenario}  ({scenario['window_minutes']}-min CEP window)")
    start_ts = resolve_start_ts(args.start_after)
    pipeline_id = args.pipeline_id or PIPELINE_IDS[0]
    print(f"  Pipeline       : {pipeline_id}")
    print(f"  Event gap      : {args.gap}s")

    events = generate_events(args.scenario, start_ts, args.count, args.gap, pipeline_id)

    if args.kafka:
        produce_to_kafka(events)
    else:
        write_to_file(events, args.output)
