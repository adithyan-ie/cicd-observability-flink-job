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

            JsonNode eventNode = root.has("event") ? root.get("event") : root;

            CicdEvent event = mapper.treeToValue(eventNode, CicdEvent.class);

            if (event.getEventTimestamp() != null && !event.getEventTimestamp().isEmpty()) {
                try {
                    event.setTimestampMs(Instant.parse(event.getEventTimestamp()).toEpochMilli());
                } catch (Exception e) {

                    event.setTimestampMs(Instant.now().toEpochMilli());
                    LOG.warn("Could not parse timestamp '{}', using processing time",
                            event.getEventTimestamp());
                }
            } else {
                event.setTimestampMs(Instant.now().toEpochMilli());
            }

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
