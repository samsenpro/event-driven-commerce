package com.example.commerce.platform.messaging;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.EventType;
import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.PaymentRequested;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventReaderTest {

    private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
    private final EventReader reader =
            new EventReader(objectMapper, Validation.buildDefaultValidatorFactory().getValidator());
    private final EventFactory factory =
            new EventFactory(Clock.fixed(Instant.parse("2026-09-23T10:00:00Z"), ZoneOffset.UTC), "test-service");

    @Test
    void readsAValidEventWithItsMetadata() throws Exception {
        EventEnvelope<PaymentRequested> original =
                factory.create(new PaymentRequested(42L, 7L, new BigDecimal("99.90")), 42L);

        EventEnvelope<PaymentRequested> read = reader.read(record(objectMapper.writeValueAsString(original)),
                EventType.PAYMENT_REQUESTED);

        assertThat(read).isEqualTo(original);
        assertThat(read.eventVersion()).isEqualTo(1);
        assertThat(read.aggregateType()).isEqualTo("Order");
        assertThat(read.source()).isEqualTo("test-service");
        assertThat(read.correlationId()).isNotBlank();
    }

    @Test
    void ignoresUnknownFieldsForForwardCompatibility() throws Exception {
        String json = objectMapper.writeValueAsString(factory.create(new PaymentRequested(1L, 1L, BigDecimal.ONE), 1L))
                .replace("\"amount\"", "\"newOptionalField\":\"x\",\"amount\"");

        EventEnvelope<PaymentRequested> read = reader.read(record(json), EventType.PAYMENT_REQUESTED);

        assertThat(read.payload().amount()).isEqualByComparingTo("1");
    }

    @Test
    void rejectsUnreadableJson() {
        assertThatThrownBy(() -> reader.read(record("{ not json"), EventType.PAYMENT_REQUESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("Unreadable");
        assertThatThrownBy(() -> reader.read(record(""), EventType.PAYMENT_REQUESTED))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void rejectsAnEventOfAnotherType() throws Exception {
        String json = objectMapper.writeValueAsString(factory.create(new PaymentRequested(1L, 1L, BigDecimal.ONE), 1L))
                .replace("PAYMENT_REQUESTED", "PAYMENT_APPROVED");

        assertThatThrownBy(() -> reader.read(record(json), EventType.PAYMENT_REQUESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("Unexpected eventType");
    }

    @Test
    void rejectsAnUnsupportedSchemaVersion() throws Exception {
        String json = objectMapper.writeValueAsString(factory.create(new PaymentRequested(1L, 1L, BigDecimal.ONE), 1L))
                .replace("\"eventVersion\":1", "\"eventVersion\":2");

        assertThatThrownBy(() -> reader.read(record(json), EventType.PAYMENT_REQUESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("Unsupported version 2");
    }

    @Test
    void rejectsAPayloadThatBreaksTheContract() throws Exception {
        String json = objectMapper.writeValueAsString(factory.create(new PaymentRequested(1L, 1L, BigDecimal.ONE), 1L))
                .replace("\"amount\":1", "\"amount\":-5");

        assertThatThrownBy(() -> reader.read(record(json), EventType.PAYMENT_REQUESTED))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("payload.amount");
    }

    @Test
    void invalidEventsAreNotRetryable() {
        assertThat(new InvalidEventException("x")).isInstanceOf(NonRetryableEventException.class);
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>(Topics.PAYMENTS_REQUESTED, 0, 0L, "42", value);
    }
}
