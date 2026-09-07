package com.cicd.observability.operators;

public final class SourceTiming {

    private SourceTiming() {}

    public static long earliest(long acc, long candidate) {
        if (candidate <= 0) return acc;
        if (acc <= 0) return candidate;
        return Math.min(acc, candidate);
    }
}
