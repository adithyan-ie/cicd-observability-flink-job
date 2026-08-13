package com.cicd.observability.operators;

/**
 * Shared helper for tracking the earliest CicdEvent.flinkReceivedAtMs
 * across the event(s) that contribute to one MetricResult row.
 *
 * Earliest (not latest) on purpose: inserted_at - this column is meant to
 * answer "how long since the FIRST relevant event was received by Flink
 * did this result take to land" — the worst-case wait for that row. Taking
 * the latest instead would let any late-pushed event sharing the window
 * (e.g. a watermark-forcing sentinel) pull the reference point toward "now"
 * and silently understate the latency of every earlier event in the same
 * window.
 *
 * 0 means "unset" (see CicdEvent.flinkReceivedAtMs / MetricResult) and
 * must be excluded from the comparison — Math.min(0, realEpochMs) would
 * otherwise always collapse to 0.
 */
public final class SourceTiming {

    private SourceTiming() {}

    /** Folds candidate into acc, ignoring either side that is 0 (unset). */
    public static long earliest(long acc, long candidate) {
        if (candidate <= 0) return acc;
        if (acc <= 0) return candidate;
        return Math.min(acc, candidate);
    }
}
