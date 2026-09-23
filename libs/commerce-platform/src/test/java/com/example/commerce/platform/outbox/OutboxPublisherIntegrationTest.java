package com.example.commerce.platform.outbox;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Publicador del outbox con PostgreSQL y Kafka reales. El caso "Kafka no disponible" usa un
 * productor que apunta a un broker inexistente.
 */
class OutboxPublisherIntegrationTest {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.9.1");
    private static final String TOPIC = "outbox.test";
    private static final String UNREACHABLE_BROKER = "localhost:1";

    private static JdbcTemplate jdbcTemplate;
    private static TransactionTemplate transactionTemplate;

    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-23T10:00:00Z"));
    private final OutboxProperties properties =
            new OutboxProperties(true, null, 10, 3, Duration.ofSeconds(2), Duration.ofSeconds(2), null);
    private OutboxStore store;

    @BeforeAll
    static void startInfrastructure() throws IOException {
        POSTGRES.start();
        KAFKA.start();
        DriverManagerDataSource dataSource =
                new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        jdbcTemplate.execute(Files.readString(Path.of("src/test/resources/outbox-schema.sql")));
    }

    @AfterAll
    static void stopInfrastructure() {
        KAFKA.stop();
        POSTGRES.stop();
    }

    @BeforeEach
    void cleanOutbox() {
        jdbcTemplate.update("DELETE FROM outbox_events");
        store = new OutboxStore(jdbcTemplate);
    }

    @Test
    void publishesPendingEventsWithTheAggregateIdAsKey() {
        UUID eventId = insert("order-1", "{\"n\":1}");

        publisher(KAFKA.getBootstrapServers()).publishPendingEvents();

        OutboxEvent published = store.findByAggregate("Order", "order-1").getFirst();
        assertThat(published.status()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(published.publishedAt()).isNotNull();
        ConsumerRecord<String, String> record = consume("order-1");
        assertThat(record.value()).isEqualTo("{\"n\":1}");
        assertThat(new String(record.headers().lastHeader("eventId").value(), StandardCharsets.UTF_8))
                .isEqualTo(eventId.toString());
    }

    @Test
    void kafkaUnavailableKeepsTheEventPendingWithBackoffUntilMaxAttempts() {
        insert("order-2", "{}");
        OutboxPublisher publisher = publisher(UNREACHABLE_BROKER);

        publisher.publishPendingEvents();
        OutboxEvent afterFirstFailure = only("order-2");
        assertThat(afterFirstFailure.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(afterFirstFailure.retryCount()).isEqualTo(1);
        assertThat(afterFirstFailure.nextAttemptAt()).isEqualTo(clock.instant().plusSeconds(2));
        assertThat(afterFirstFailure.lastError()).isNotBlank();

        // Durante el backoff no se reintenta
        publisher.publishPendingEvents();
        assertThat(only("order-2").retryCount()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(2));
        publisher.publishPendingEvents();
        assertThat(only("order-2").retryCount()).isEqualTo(2);

        clock.advance(Duration.ofSeconds(4));
        publisher.publishPendingEvents();
        assertThat(only("order-2").status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(only("order-2").retryCount()).isEqualTo(3);
    }

    @Test
    void pendingEventIsPublishedOnceKafkaRecovers() {
        insert("order-3", "{\"recovered\":true}");
        publisher(UNREACHABLE_BROKER).publishPendingEvents();
        assertThat(only("order-3").status()).isEqualTo(OutboxStatus.PENDING);

        clock.advance(Duration.ofSeconds(2));
        publisher(KAFKA.getBootstrapServers()).publishPendingEvents();

        assertThat(only("order-3").status()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(consume("order-3").value()).isEqualTo("{\"recovered\":true}");
    }

    @Test
    void laterEventsOfTheSameAggregateWaitForTheEarlierOne() {
        insert("order-4", "{\"step\":1}");
        insert("order-4", "{\"step\":2}");
        insert("order-5", "{\"other\":true}");

        List<OutboxEvent> firstBatch = transactionTemplate.execute(status ->
                store.lockNextBatch(10, clock.instant()));

        assertThat(firstBatch).extracting(OutboxEvent::payload)
                .containsExactly("{\"step\":1}", "{\"other\":true}");

        publisher(KAFKA.getBootstrapServers()).publishPendingEvents();
        assertThat(store.findByAggregate("Order", "order-4")).extracting(OutboxEvent::status)
                .containsExactly(OutboxStatus.PUBLISHED, OutboxStatus.PUBLISHED);
        List<String> values = consumeAll("order-4", 2).stream().map(ConsumerRecord::value).toList();
        assertThat(values).containsExactly("{\"step\":1}", "{\"step\":2}");
    }

    private UUID insert(String aggregateId, String payload) {
        UUID id = UUID.randomUUID();
        transactionTemplate.executeWithoutResult(status -> store.insert(id, "Order", aggregateId, "ORDER_CREATED",
                TOPIC, payload, "cid", clock.instant()));
        return id;
    }

    private OutboxEvent only(String aggregateId) {
        return store.findByAggregate("Order", aggregateId).getFirst();
    }

    private OutboxPublisher publisher(String bootstrapServers) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1500)));
        return new OutboxPublisher(store, template, transactionTemplate, properties, clock);
    }

    private ConsumerRecord<String, String> consume(String key) {
        return consumeAll(key, 1).getFirst();
    }

    private List<ConsumerRecord<String, String>> consumeAll(String key, int expected) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(TOPIC));
            Instant deadline = Instant.now().plusSeconds(15);
            while (matches.size() < expected && Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofMillis(200)).forEach(record -> {
                    if (key.equals(record.key())) {
                        matches.add(record);
                    }
                });
            }
        }
        assertThat(matches).hasSize(expected);
        return matches;
    }

    private static final class MutableClock extends Clock {

        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
