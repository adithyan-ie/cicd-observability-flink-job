package com.cicd.observability.deserializer;

import com.cicd.observability.model.CicdEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

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
 *
 * CicdEvent.flinkReceivedAtMs is stamped here via System.currentTimeMillis(),
 * never from anything in the JSON payload: a per-record field would let a
 * buggy or malicious producer (a Jenkins job, a manual kcat push, a
 * load-test script with a clock bug) forge or omit its own "when I sent
 * this" claim. This is the trustworthy source for Flink's own processing
 * latency (inserted_at - flinkReceivedAtMs in cicd_metrics).
 */
public class CicdEventDeserializer implements KafkaRecordDeserializationSchema<CicdEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CicdEventDeserializer.class);

    private transient ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CicdEvent> out) {
        byte[] message = record.value();
        if (message == null || message.length == 0) return;
        try {
            JsonNode root = mapper.readTree(message);

            // Handle both wrapped {"event": {...}} and flat {...} payloads
            JsonNode eventNode = root.has("event") ? root.get("event") : root;

            CicdEvent event = mapper.treeToValue(eventNode, CicdEvent.class);

            // Parse ISO-8601 timestamp → epoch milliseconds for Flink watermark
            if (event.getEventTimestamp() != null && !event.getEventTimestamp().isEmpty()) {
                try {
                    event.setTimestampMs(Instant.parse(event.getEventTimestamp()).toEpochMilli());
                } catch (Exception e) {
                    // Fall back to processing time
                    event.setTimestampMs(Instant.now().toEpochMilli());
                    LOG.warn("Could not parse timestamp '{}', using processing time",
                            event.getEventTimestamp());
                }
            } else {
                event.setTimestampMs(Instant.now().toEpochMilli());
            }

            // Wall-clock time this record was actually deserialised by
            // Flink — see class javadoc for why this always wins over
            // anything the producer put in the payload.
            event.setFlinkReceivedAtMs(System.currentTimeMillis());

            out.collect(event);

        } catch (Exception e) {
            LOG.error("Failed to deserialise event: {}", new String(message), e);
        }
    }

    @Override
    public TypeInformation<CicdEvent> getProducedType() {
        return TypeInformation.of(CicdEvent.class);
    }
}
