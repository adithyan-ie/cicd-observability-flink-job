package com.cicd.observability.config;

import com.cicd.observability.deserializer.CicdEventDeserializer;
import com.cicd.observability.model.CicdEvent;
import com.cicd.observability.model.MetricResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Properties;

/**
 * Central Flink + Kafka configuration.
 *
 * RocksDB state backend is NOW enabled programmatically here
 * (previously it was only in flink-conf.yaml which is advisory,
 *  not enforced at runtime).
 *
 * RocksDB is required for stateful operators:
 *   - LeadTimeProcessFn (MapState: commitSha → startMs)
 *   - MttrProcessFn     (ValueState: failure start ms)
 *   - CEP NFA state     (managed by Flink CEP internally)
 * Without RocksDB these states live on heap and will OOM
 * under sustained pipeline load.
 */
public class FlinkConfig {

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? defaultValue : v;
    }

    // ── Kafka ──────────────────────────────────────────────────────────
    public static final String KAFKA_BOOTSTRAP       = env("KAFKA_BOOTSTRAP", "kafka:29092");
    public static final String TOPIC_CICD_EVENTS     = "cicd-events";
    public static final String TOPIC_METRICS         = "pipeline-metrics";
    public static final String TOPIC_HEALTH          = "pipeline-health";
    public static final String TOPIC_FAILURE_ALERTS  = "failure-pattern-alerts";
    public static final String TOPIC_PATTERN_TIMEOUT = "pattern-timeouts";
    public static final String TOPIC_LATE_EVENTS     = "late-events";
    public static final String CONSUMER_GROUP        = "flink-cicd-analytics";

    // ── Watermark ──────────────────────────────────────────────────────
    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(30);
    public static final Duration IDLE_TIMEOUT         = Duration.ofSeconds(60);

    // ── Live-metric late gate ─────────────────────────────────────────
    //
    // The live counters (LiveDeployCounter / LiveCfrCounter / LiveHealthCounter)
    // decide "has my window closed?" from per-key state alone: they only roll
    // over when THAT key's own traffic carries a timestamp past the boundary.
    // The watermark, however, is job-global — one pipeline's future-dated
    // event (e.g. a manually-pushed sentinel used to force a window to fire
    // early in testing) advances it for every pipeline. Any other pipeline
    // that then receives a "today" event which is genuinely late relative to
    // that now-advanced watermark has no per-key memory of this: its stored
    // windowEnd is still "today", so the late event looks perfectly current
    // and gets counted into live — when only the historical stream (with its
    // allowedLateness grace period) should reflect it.
    //
    // When true, each live counter additionally rejects an event whose own
    // window has already been passed by ctx.timerService().currentWatermark(),
    // regardless of that key's local state. When false, the counters fall
    // back to the original per-key-only check (kept for comparison).
    public static final boolean LIVE_COUNTER_WATERMARK_GATE =
            Boolean.parseBoolean(env("LIVE_COUNTER_WATERMARK_GATE", "true"));

    // ── Windows ────────────────────────────────────────────────────────
    //
    // Single source of truth per metric, shared by that metric's historical
    // compute() window AND its computeLive() bucket — they must always
    // match, or the live tile can never accumulate past 1 event before
    // resetting (see DeploymentFrequencyOperator / PipelineHealthOperator
    // computeLive() javadoc).
    public static final Time DEPLOYMENT_FREQUENCY_WINDOW = Time.days(1);
    public static final Time CHANGE_FAILURE_RATE_WINDOW  = Time.days(1);
    public static final Time PIPELINE_HEALTH_WINDOW      = Time.minutes(30);

    // ── CEP ────────────────────────────────────────────────────────────
    public static final Duration CEP_PATTERN_WINDOW = Duration.ofMinutes(10);

    // ── Postgres ───────────────────────────────────────────────────────
    public static final String PG_URL      = env("PG_URL", "jdbc:postgresql://postgres:5432/cicd_metrics");
    public static final String PG_USER     = env("PG_USER", "flink");
    public static final String PG_PASSWORD = env("PG_PASSWORD", "flink_secret");

    // ── Grafana ───────────────────────────────────────────────────────
    public static final String GRAFANA_URL     = env("GRAFANA_URL", "http://grafana:3000");
    public static final String GRAFANA_API_KEY = env("GRAFANA_API_KEY", "your-grafana-api-key");
    public static final String GRAFANA_DS_NAME = "PostgreSQL";

    // ═══════════════════════════════════════════════════════════════════
    // Flink environment — RocksDB state backend enabled here
    // ═══════════════════════════════════════════════════════════════════

    public static StreamExecutionEnvironment createEnvironment() {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ── RocksDB state backend (programmatic — overrides flink-conf.yaml) ──
        //
        // EmbeddedRocksDBStateBackend stores state on local disk (SSD in prod)
        // rather than JVM heap.  This is REQUIRED for:
        //   - MapState in LeadTimeProcessFn (can hold thousands of in-flight commits)
        //   - ValueState in MttrProcessFn (one per pipeline)
        //   - CEP NFA state (Flink manages this internally using keyed state)
        //
        // incremental=true means only state CHANGES are written per checkpoint,
        // not the full state.  Critical for large state (e.g. many pipelines).
        //
        // Without this call, Flink uses HashMapStateBackend (heap) by default,
        // which will OOM under sustained pipeline event load.
        EmbeddedRocksDBStateBackend rocksDb =
                new EmbeddedRocksDBStateBackend(true /* incremental */);
        env.setStateBackend(rocksDb);

        // Checkpoint directory — override in K8s via env var / flink-conf.yaml
        // For production use S3/GCS:
        //   env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/checkpoints");
        env.getCheckpointConfig().setCheckpointStorage(
                "file:///tmp/flink-checkpoints");

        // ── Checkpointing ──────────────────────────────────────────────
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig cpCfg = env.getCheckpointConfig();
        cpCfg.setMinPauseBetweenCheckpoints(5_000);
        cpCfg.setCheckpointTimeout(120_000);
        cpCfg.setMaxConcurrentCheckpoints(1);
        // Default is 0 — a single slow checkpoint (e.g. during Kafka backlog
        // catch-up, when many window firings burst through the synchronous
        // Postgres sink) would otherwise trigger a full job restart, which
        // replays the backlog and risks repeating the same slow checkpoint.
        cpCfg.setTolerableCheckpointFailureNumber(3);

        // Retain the last checkpoint on job cancellation so state is not lost
        cpCfg.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Emit a synthetic LatencyMarker from each source every 1s, timestamped
        // at injection. Downstream operators report marker-arrival latency to
        // the metrics reporter — see latencyTracking.* in Flink's web UI/REST API.
        env.getConfig().setLatencyTrackingInterval(1000);

        return env;
    }

    // ── Watermark strategy ─────────────────────────────────────────────

    public static WatermarkStrategy<CicdEvent> watermarkStrategy() {
        return WatermarkStrategy
                .<CicdEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, ts) -> event.getTimestampMs())
                .withIdleness(IDLE_TIMEOUT);
    }

    // ── Kafka source ───────────────────────────────────────────────────

    public static KafkaSource<CicdEvent> kafkaSource() {
        Properties props = new Properties();
        props.setProperty("max.poll.records", "500");
        props.setProperty("fetch.max.wait.ms", "500");

        return KafkaSource.<CicdEvent>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC_CICD_EVENTS)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new CicdEventDeserializer())
                .setProperties(props)
                .build();
    }

    // ── Kafka sinks ────────────────────────────────────────────────────

    public static KafkaSink<MetricResult> metricSink(String topic) {
        return KafkaSink.<MetricResult>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(new MetricResultSerializer(topic))
                .build();
    }

    public static KafkaSink<String> stringSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(
                                        (SerializationSchema<String>) v ->
                                                v.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                                .build())
                .build();
    }

    // ── MetricResult serialiser ────────────────────────────────────────

    static class MetricResultSerializer
            implements KafkaRecordSerializationSchema<MetricResult> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        MetricResultSerializer(String topic) { this.topic = topic; }

        @Override
        public void open(SerializationSchema.InitializationContext ctx,
                         KafkaSinkContext sinkCtx) {
            mapper = new ObjectMapper();
        }

        @Override
        @Nullable
        public ProducerRecord<byte[], byte[]> serialize(
                MetricResult m, KafkaSinkContext ctx, Long ts) {
            try {
                String key = m.getServiceName() + ":" + m.getMetricType();
                return new ProducerRecord<>(topic, key.getBytes(),
                        mapper.writeValueAsBytes(m));
            } catch (Exception e) { return null; }
        }
    }
}
