package com.cicd.observability.sink;

import com.cicd.observability.config.FlinkConfig;
import com.cicd.observability.model.MetricResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Grafana sink — pushes metric results to Grafana via two APIs:
 *
 *  1. Grafana Annotations API (/api/annotations)
 *     Used for discrete events: CEP pattern matches, late event detections,
 *     performance band changes.  These show as vertical lines in dashboards.
 *
 *  2. Grafana HTTP data source / JSON push endpoint
 *     Grafana itself doesn't have a native push endpoint for time-series,
 *     so we use the SimpleJson data source plugin or push to InfluxDB/Postgres
 *     which Grafana then reads.  This sink uses the Annotations API for
 *     direct Grafana pushes and relies on Postgres for the time-series panels.
 *
 * What Grafana shows:
 *   - Annotations: pattern detections, DORA band changes, late event spikes
 *   - Dashboards (via Postgres data source): all MetricResult values over time
 *
 * Note: For production, Grafana dashboards should be provisioned via
 *   grafana/dashboards/*.json in your Helm chart and read from Postgres.
 *   This sink handles real-time annotations only.
 */
public class GrafanaSink extends RichSinkFunction<MetricResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GrafanaSink.class);

    private final String grafanaUrl;
    private final String apiKey;

    private transient HttpClient  httpClient;
    private transient ObjectMapper mapper;

    // ── Constructors ───────────────────────────────────────────────────

    public GrafanaSink() {
        this(FlinkConfig.GRAFANA_URL, FlinkConfig.GRAFANA_API_KEY);
    }

    public GrafanaSink(String grafanaUrl, String apiKey) {
        this.grafanaUrl = grafanaUrl;
        this.apiKey     = apiKey;
    }

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        mapper = new ObjectMapper();
        LOG.info("GrafanaSink: connected to {}", grafanaUrl);
    }

    // ── Main invoke ────────────────────────────────────────────────────

    @Override
    public void invoke(MetricResult metric, Context ctx) throws Exception {
        if (metric == null || metric.getMetricType() == null) return;

        // Decide whether this metric warrants a Grafana annotation.
        // Not every metric needs one — only significant events.
        if (shouldAnnotate(metric)) {
            postAnnotation(metric);
        }
    }

    // ── Annotation logic ───────────────────────────────────────────────

    /**
     * Only post annotations for significant events, not every metric point.
     * Posting every DORA data point as an annotation would clutter dashboards.
     */
    private boolean shouldAnnotate(MetricResult metric) {
        switch (metric.getMetricType()) {
            case FAILURE_PATTERN_DETECTED:
            case PATTERN_TIMEOUT:
            case LATE_EVENT_DETECTED:
                return true;   // Always annotate discrete alerts
            case PIPELINE_HEALTH_SCORE:
                return "Low".equals(metric.getPerformanceBand())
                    || "Elite".equals(metric.getPerformanceBand());
            case DEPLOYMENT_FREQUENCY:
            case LEAD_TIME_FOR_CHANGES:
            case CHANGE_FAILURE_RATE:
            case MEAN_TIME_TO_RECOVERY:
                return true;   // Always annotate DORA metrics, regardless of band
            default:
                return false;
        }
    }

    /**
     * Posts an annotation to Grafana using the Annotations REST API.
     *
     * Grafana Annotations API:
     *   POST /api/annotations
     *   {
     *     "time":     <epoch ms>,
     *     "timeEnd":  <epoch ms>,   // optional — for range annotations
     *     "tags":     ["cicd", "dora", "pipeline-id"],
     *     "text":     "Human-readable description",
     *     "dashboardUID": "optional — pin to specific dashboard"
     *   }
     *
     * The annotation appears as a vertical line (or region) in all Grafana
     * dashboards that have annotations enabled, tagged with the metric type.
     */
    private void postAnnotation(MetricResult metric) {
        try {
            Map<String, Object> annotation = new HashMap<>();
//            annotation.put("time",    metric.getWindowEndMs() > 0
//                    ? metric.getWindowEndMs() : metric.getComputedAtMs());
            annotation.put("tags",    buildTags(metric));
            annotation.put("text",    buildText(metric));

            // For range annotations (e.g. MTTR duration)
//            if (metric.getWindowStartMs() > 0 && metric.getWindowEndMs() > 0
//                    && metric.getWindowEndMs() > metric.getWindowStartMs()) {
//                annotation.put("timeEnd", metric.getWindowEndMs());
//            }

            String body = mapper.writeValueAsString(annotation);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(grafanaUrl + "/api/annotations"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                LOG.debug("Grafana annotation posted: type={} pipeline={}",
                        metric.getMetricType(), metric.getPipelineId());
            } else {
                LOG.warn("Grafana annotation failed: status={} body={}",
                        response.statusCode(), response.body());
            }

        } catch (Exception e) {
            // Don't crash the Flink job if Grafana is unavailable —
            // metrics are already in Postgres and Kafka.
            LOG.warn("Failed to post Grafana annotation (non-fatal): {}", e.getMessage());
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private List<String> buildTags(MetricResult metric) {
        return List.of(
                "cicd-observability",
                metric.getMetricType().name().toLowerCase().replace('_', '-'),
                "pipeline:" + nullSafe(metric.getPipelineId()),
                "service:"  + nullSafe(metric.getServiceName()),
                "band:"     + nullSafe(metric.getPerformanceBand())
        );
    }

    private String buildText(MetricResult metric) {
        return String.format(
                "[%s] %s — pipeline: %s | value: %.2f | band: %s",
                metric.getMetricType(),
                nullSafe(metric.getServiceName()),
                nullSafe(metric.getPipelineId()),
                metric.getValue(),
                nullSafe(metric.getPerformanceBand()));
    }

    private static String nullSafe(String s) { return s == null ? "?" : s; }

    @Override
    public void close() {
        LOG.info("GrafanaSink: closed");
    }
}
