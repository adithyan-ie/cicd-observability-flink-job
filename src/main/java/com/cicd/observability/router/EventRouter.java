package com.cicd.observability.router;

import com.cicd.observability.model.CicdEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class EventRouter extends ProcessFunction<CicdEvent, CicdEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(EventRouter.class);

    private static final long LATE_CANDIDATE_THRESHOLD_MS = 30_000L;

    public static final OutputTag<CicdEvent> DORA_TAG =
            new OutputTag<CicdEvent>("dora-events") {};

    public static final OutputTag<CicdEvent> HEALTH_TAG =
            new OutputTag<CicdEvent>("health-events") {};

    public static final OutputTag<CicdEvent> LATE_TAG =
            new OutputTag<CicdEvent>("late-candidate-events") {};

    public static final OutputTag<CicdEvent> CEP_TAG =
            new OutputTag<CicdEvent>("cep-pattern-events") {};

    private static final Set<String> DORA_EVENT_TYPES = Set.of(
            "BUILD_STARTED",
            "BUILD_SUCCESS",
            "BUILD_FAILED",
            "DEPLOY_STARTED",
            "DEPLOY_SUCCESS",
            "DEPLOY_FAILED"
    );

    private static final Set<String> HEALTH_EVENT_TYPES = Set.of(
            "BUILD_STARTED",  "BUILD_SUCCESS",  "BUILD_FAILED",
            "TEST_STARTED",   "TEST_SUCCESS",   "TEST_FAILED",
            "SONARQUBE_STARTED", "SONARQUBE_SUCCESS", "SONARQUBE_FAILED",
            "PACKAGE_STARTED","PACKAGE_SUCCESS","PACKAGE_FAILED",
            "DEPLOY_STARTED", "DEPLOY_SUCCESS", "DEPLOY_FAILED"
    );

    private static final Set<String> CEP_EVENT_TYPES = Set.of(
            "BUILD_SUCCESS",
            "DEPLOY_STARTED",
            "DEPLOY_FAILED",
            "ROLLBACK_STARTED"
    );

    @Override
    public void processElement(CicdEvent event,
                               Context ctx,
                               Collector<CicdEvent> out) {

        if (event == null || event.getEventType() == null) return;

        String eventType  = event.getEventType();
        boolean routed    = false;
        long nowMs        = System.currentTimeMillis();
        long eventMs      = event.getTimestampMs();

        if (DORA_EVENT_TYPES.contains(eventType)) {
            ctx.output(DORA_TAG, event);
            routed = true;
            LOG.debug("→ DORA: pipeline={} type={}", event.getPipelineId(), eventType);
        }

        if (HEALTH_EVENT_TYPES.contains(eventType)) {
            ctx.output(HEALTH_TAG, event);
            routed = true;
            LOG.debug("→ HEALTH: pipeline={} type={}", event.getPipelineId(), eventType);
        }

        if (eventMs > 0 && (nowMs - eventMs) > LATE_CANDIDATE_THRESHOLD_MS) {
            ctx.output(LATE_TAG, event);
            routed = true;
            LOG.info("→ LATE: pipeline={} type={} delay={}s",
                    event.getPipelineId(), eventType,
                    (nowMs - eventMs) / 1000);
        }

        if (CEP_EVENT_TYPES.contains(eventType)) {
            ctx.output(CEP_TAG, event);
            routed = true;
            LOG.debug("→ CEP: pipeline={} type={}", event.getPipelineId(), eventType);
        }

        out.collect(event);

        if (!routed) {
            LOG.debug("Event not routed to any use case: type={}", eventType);
        }
    }
}
