package com.cicd.observability.config;

import com.cicd.observability.deserializer.CicdEventDeserializer;
import com.cicd.observability.model.CicdEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class FlinkConfig {

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? defaultValue : v;
    }

    public static final String KAFKA_BOOTSTRAP       = env("KAFKA_BOOTSTRAP", "kafka:29092");
    public static final String TOPIC_CICD_EVENTS     = "cicd-events";
    public static final String CONSUMER_GROUP        = "flink-cicd-analytics";

    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    public static final Duration IDLE_TIMEOUT         = Duration.ofSeconds(30);

    public static final boolean LIVE_COUNTER_WATERMARK_GATE =
            Boolean.parseBoolean(env("LIVE_COUNTER_WATERMARK_GATE", "true"));

    public static final Time DEPLOYMENT_FREQUENCY_WINDOW = Time.days(1);
    public static final Time CHANGE_FAILURE_RATE_WINDOW  = Time.days(1);
    public static final Time PIPELINE_HEALTH_WINDOW      = Time.minutes(30);

    public static final Duration CEP_PATTERN_WINDOW = Duration.ofMinutes(10);

    public static final String PG_URL      = env("PG_URL", "jdbc:postgresql://postgres:5432/cicd_metrics");
    public static final String PG_USER     = env("PG_USER", "flink");
    public static final String PG_PASSWORD = env("PG_PASSWORD", "flink_secret");

    public static final String GRAFANA_URL     = env("GRAFANA_URL", "http://grafana:3000");
    public static final String GRAFANA_API_KEY = env("GRAFANA_API_KEY", "your-grafana-api-key");
    public static final String GRAFANA_DS_NAME = "PostgreSQL";

    public static StreamExecutionEnvironment createEnvironment() {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        EmbeddedRocksDBStateBackend rocksDb =
                new EmbeddedRocksDBStateBackend(true );
        env.setStateBackend(rocksDb);

        env.getCheckpointConfig().setCheckpointStorage(
                "file:///tmp/flink-checkpoints");

        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig cpCfg = env.getCheckpointConfig();
        cpCfg.setMinPauseBetweenCheckpoints(5_000);
        cpCfg.setCheckpointTimeout(120_000);
        cpCfg.setMaxConcurrentCheckpoints(1);

        cpCfg.setTolerableCheckpointFailureNumber(3);

        cpCfg.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getConfig().setLatencyTrackingInterval(1000);

        return env;
    }

    public static WatermarkStrategy<CicdEvent> watermarkStrategy() {
        return WatermarkStrategy
                .<CicdEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, ts) -> event.getTimestampMs())
                .withIdleness(IDLE_TIMEOUT);
    }

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

}
