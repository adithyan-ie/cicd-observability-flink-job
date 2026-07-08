package com.cicd.observability.deserializer;

import com.cicd.observability.model.CicdEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Deserialises the compact JSON produced by the Jenkins stageEvent() function.
 *
 * The Jenkins pipeline wraps the event inside a top-level object:
 * {
 *   "analysis_name": "...",
 *   "event": { <-- actual CicdEvent fields live here
 *     "event_id": "...",
 *     "event_type": "BUILD_STARTED",
 *     ...
 *   }
 * }
 *
 * This deserialiser unwraps the "event" node and maps it to CicdEvent,
 * then parses event_timestamp → timestampMs for Flink event-time processing.
 */
public class CicdEventDeserializer implements DeserializationSchema<CicdEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CicdEventDeserializer.class);
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public CicdEvent deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) return null;
        try {
            JsonNode root = mapper.readTree(message);

            // Handle both wrapped {"event": {...}} and flat {...} payloads
            JsonNode eventNode = root.has("event") ? root.get("event") : root;

            CicdEvent event = mapper.treeToValue(eventNode, CicdEvent.class);

            // Parse ISO-8601 timestamp → epoch milliseconds for Flink watermark
            if (event.getEventTimestamp() != null && !event.getEventTimestamp().isEmpty()) {
                try {
                    event.setTimestampMs(LocalDateTime.parse(event.getEventTimestamp()).toInstant(ZoneOffset.UTC).toEpochMilli());
                } catch (Exception e) {
                    // Fall back to processing time
                    event.setTimestampMs(Instant.now().toEpochMilli());
                    LOG.warn("Could not parse timestamp '{}', using processing time",
                            event.getEventTimestamp());
                }
            } else {
                event.setTimestampMs(Instant.now().toEpochMilli());
            }

            return event;

        } catch (Exception e) {
            LOG.error("Failed to deserialise event: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(CicdEvent event) {
        return false; // unbounded stream
    }

    @Override
    public TypeInformation<CicdEvent> getProducedType() {
        return TypeInformation.of(CicdEvent.class);
    }
}
