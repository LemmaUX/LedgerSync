package com.ledgersync.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinks;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Event Normalizer Job - Flink streaming application that:
 * 1. Reads CDC events from Kafka (Avro format)
 * 2. Normalizes them to the canonical CanonicalTransaction schema
 * 3. Writes normalized events back to Kafka
 * 
 * This job serves as the first stage in the LedgerSync reconciliation pipeline,
 * ensuring all transactions conform to a unified schema before reconciliation.
 */
public class EventNormalizerJob {

    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String INPUT_TOPIC = "ledger.internal_ledger.transactions";
    private static final String OUTPUT_TOPIC = "ledger.canonical.transactions";
    private static final String CONSUMER_GROUP_ID = "event-normalizer";
    private static final long CHECKPOINT_INTERVAL_MS = 10000L; // 10 seconds

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        
        // Configure watermark strategy for event-time processing
        // CDC events should arrive in order, but we allow 5 seconds of lateness
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> {
                // Extract timestamp from event JSON (occurred_at field)
                // For now, use processing time; will be updated with Avro deserialization
                return System.currentTimeMillis();
            });

        // Kafka source: read CDC events from internal ledger
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(INPUT_TOPIC)
            .setGroupId(CONSUMER_GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawEvents = env.fromSource(
            source,
            watermarkStrategy,
            "Kafka Source - Internal Ledger CDC"
        );

        // Normalize events: transform source-specific schema to canonical schema
        DataStream<String> normalizedEvents = rawEvents
            .flatMap(new NormalizerFunction())
            .name("Event Normalizer");

        // Kafka sink: write normalized events to canonical topic
        KafkaSink<String> sink = KafkaSinks.sink()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setRecordSerializer(new KafkaRecordSerializer<String>() {
                @Override
                public byte[] serializeKey(String element) {
                    // Use transaction_id as key for partitioning
                    // This ensures same transaction always goes to same partition
                    return extractTransactionId(element).getBytes();
                }

                @Override
                public byte[] serializeValue(String element) {
                    return element.getBytes();
                }
            })
            .setTopic(OUTPUT_TOPIC)
            .build();

        normalizedEvents.sinkTo(sink).name("Kafka Sink - Canonical Transactions");

        // Execute the job
        env.execute("LedgerSync Event Normalizer");
    }

    /**
     * Extract transaction ID from JSON event for keying.
     * In production, this would parse the actual Avro record.
     */
    private static String extractTransactionId(String jsonEvent) {
        // Placeholder implementation - will be replaced with proper Avro parsing
        // Expected format: {"uuid": "a1b2c3d4-...", ...}
        if (jsonEvent.contains("\"uuid\"")) {
            int start = jsonEvent.indexOf("\"uuid\"") + 8;
            int end = jsonEvent.indexOf("\"", start);
            if (end > start) {
                return jsonEvent.substring(start, end);
            }
        }
        return "unknown";
    }
}
