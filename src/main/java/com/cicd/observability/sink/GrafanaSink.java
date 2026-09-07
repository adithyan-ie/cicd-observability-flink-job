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

public class GrafanaSink extends RichSinkFunction<MetricResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GrafanaSink.class);

    private final String grafanaUrl;
    private final String apiKey;

    private transient HttpClient  httpClient;
    private transient ObjectMapper mapper;

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

    @Override
    public void invoke(MetricResult metric, Context ctx) throws Exception {
        if (metric == null || metric.getMetricType() == null) return;

        if (shouldAnnotate(metric)) {
            postAnnotation(metric);
        }
    }

    private boolean shouldAnnotate(MetricResult metric) {
        switch (metric.getMetricType()) {
            case FAILURE_PATTERN_DETECTED:
            case PATTERN_TIMEOUT:
            case LATE_EVENT_DETECTED:
            case DEPLOYMENT_FREQUENCY_LATE_EVENTS:
            case CHANGE_FAILURE_RATE_LATE_EVENTS:
            case PIPELINE_HEALTH_SCORE_LATE_EVENTS:
                return true;
            case PIPELINE_HEALTH_SCORE:
                return "Low".equals(metric.getPerformanceBand())
                    || "Elite".equals(metric.getPerformanceBand());
            case DEPLOYMENT_FREQUENCY:
            case LEAD_TIME_FOR_CHANGES:
            case CHANGE_FAILURE_RATE:
            case MEAN_TIME_TO_RECOVERY:
                return true;
            default:
                return false;
        }
    }

    private void postAnnotation(MetricResult metric) {
        try {
            Map<String, Object> annotation = new HashMap<>();

            annotation.put("tags",    buildTags(metric));
            annotation.put("text",    buildText(metric));

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

            LOG.warn("Failed to post Grafana annotation (non-fatal): {}", e.getMessage());
        }
    }

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
