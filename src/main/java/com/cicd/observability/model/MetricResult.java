package com.cicd.observability.model;

import java.io.Serializable;

/**
 * Generic output model for all computed metrics.
 * Serialised to JSON and sunk to Kafka output topics.
 */
public class MetricResult implements Serializable {

    public enum MetricType {
        // DORA
        DEPLOYMENT_FREQUENCY,
        LEAD_TIME_FOR_CHANGES,
        CHANGE_FAILURE_RATE,
        MEAN_TIME_TO_RECOVERY,
        // Health
        PIPELINE_HEALTH_SCORE,
        // Late events
        LATE_EVENT_DETECTED,
        LATE_EVENT_CORRECTED,
        // CEP
        FAILURE_PATTERN_DETECTED,
        PATTERN_TIMEOUT
    }

    private MetricType metricType;
    private String     pipelineId;
    private String     serviceName;
    private long       windowStartMs;
    private long       windowEndMs;
    private double     value;
    private String     performanceBand;   // Elite / High / Medium / Low
    private long       sampleCount;
    private String     detail;            // JSON blob for complex results
    private long       computedAtMs;

    public MetricResult() {
        this.computedAtMs = System.currentTimeMillis();
    }

    public MetricResult(MetricType type, String pipelineId, String serviceName,
                        long windowStart, long windowEnd, double value, long samples) {
        this();
        this.metricType   = type;
        this.pipelineId   = pipelineId;
        this.serviceName  = serviceName;
        this.windowStartMs = windowStart;
        this.windowEndMs   = windowEnd;
        this.value        = value;
        this.sampleCount  = samples;
        this.performanceBand = classifyBand(type, value);
    }

    private static String classifyBand(MetricType type, double value) {
        switch (type) {
            case DEPLOYMENT_FREQUENCY:
                if (value >= 1.0)    return "Elite";
                if (value >= 1.0/7)  return "High";
                if (value >= 1.0/30) return "Medium";
                return "Low";
            case LEAD_TIME_FOR_CHANGES:
                if (value < 60)    return "Elite";
                if (value < 1440)  return "High";
                if (value < 10080) return "Medium";
                return "Low";
            case CHANGE_FAILURE_RATE:
                if (value <= 5)  return "Elite";
                if (value <= 10) return "High";
                if (value <= 15) return "Medium";
                return "Low";
            case MEAN_TIME_TO_RECOVERY:
                if (value < 60)    return "Elite";
                if (value < 1440)  return "High";
                if (value < 10080) return "Medium";
                return "Low";
            case PIPELINE_HEALTH_SCORE:
                if (value >= 85) return "Elite";
                if (value >= 70) return "High";
                if (value >= 50) return "Medium";
                return "Low";
            default: return "N/A";
        }
    }

    // ── Getters / Setters ──────────────────────────────────────────────
    public MetricType getMetricType()            { return metricType; }
    public void       setMetricType(MetricType v){ this.metricType = v; }
    public String  getPipelineId()               { return pipelineId; }
    public void    setPipelineId(String v)        { this.pipelineId = v; }
    public String  getServiceName()              { return serviceName; }
    public void    setServiceName(String v)       { this.serviceName = v; }
    public long    getWindowStartMs()            { return windowStartMs; }
    public void    setWindowStartMs(long v)      { this.windowStartMs = v; }
    public long    getWindowEndMs()              { return windowEndMs; }
    public void    setWindowEndMs(long v)        { this.windowEndMs = v; }
    public double  getValue()                    { return value; }
    public void    setValue(double v)            { this.value = v; }
    public String  getPerformanceBand()          { return performanceBand; }
    public void    setPerformanceBand(String v)  { this.performanceBand = v; }
    public long    getSampleCount()              { return sampleCount; }
    public void    setSampleCount(long v)        { this.sampleCount = v; }
    public String  getDetail()                   { return detail; }
    public void    setDetail(String v)           { this.detail = v; }
    public long    getComputedAtMs()             { return computedAtMs; }
    public void    setComputedAtMs(long v)       { this.computedAtMs = v; }

    @Override
    public String toString() {
        return "MetricResult{type=" + metricType + ", pipeline=" + pipelineId
                + ", value=" + value + ", band=" + performanceBand + "}";
    }
}
