package com.example.commerce.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Lee un topic desde el principio con un consumer group propio y efímero (no interfiere con los
 * consumer groups de la aplicación) y espera a que aparezcan los mensajes buscados.
 */
public class TopicProbe {

    private static final Duration POLL = Duration.ofMillis(200);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);

    private final String bootstrapServers;
    private final ObjectMapper objectMapper;

    TopicProbe(String bootstrapServers, ObjectMapper objectMapper) {
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = objectMapper;
    }

    /** Espera el primer mensaje del topic cuyo {@code aggregateId} (clave) coincide. */
    public ConsumerRecord<String, String> awaitRecord(String topic, Object aggregateId) {
        return awaitRecords(topic, record -> String.valueOf(aggregateId).equals(record.key()), 1, DEFAULT_TIMEOUT)
                .getFirst();
    }

    public List<ConsumerRecord<String, String>> awaitRecords(String topic, Predicate<ConsumerRecord<String, String>> filter,
                                                             int expected, Duration timeout) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline) && matches.size() < expected) {
                consumer.poll(POLL).forEach(record -> {
                    if (filter.test(record)) {
                        matches.add(record);
                    }
                });
            }
        }
        if (matches.size() < expected) {
            throw new AssertionError("Expected " + expected + " matching records on " + topic + " but found "
                    + matches.size());
        }
        return matches;
    }

    /** Todos los mensajes que coinciden y llegan dentro de la ventana de observación. */
    public List<ConsumerRecord<String, String>> collect(String topic, Predicate<ConsumerRecord<String, String>> filter,
                                                        Duration window) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(window);
            while (Instant.now().isBefore(deadline)) {
                consumer.poll(POLL).forEach(record -> {
                    if (filter.test(record)) {
                        matches.add(record);
                    }
                });
            }
        }
        return matches;
    }

    public JsonNode body(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readTree(record.value());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private KafkaConsumer<String, String> newConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
}
