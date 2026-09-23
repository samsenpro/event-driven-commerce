package com.example.commerce.testing;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica en Kafka los eventos que en producción emitirían los otros servicios, para probar
 * cada servicio de forma aislada con Kafka real.
 */
public class EventSender {

    private final String bootstrapServers;
    private final ObjectMapper objectMapper;

    EventSender(String bootstrapServers, ObjectMapper objectMapper) {
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = objectMapper;
    }

    public <T> EventEnvelope<T> envelope(T payload, Object aggregateId, String correlationId) {
        EventType type = EventType.forPayload(payload);
        return new EventEnvelope<>(UUID.randomUUID(), type.name(), type.version(), Instant.now(),
                type.aggregateType(), String.valueOf(aggregateId), correlationId, "test", payload);
    }

    /** Publica el payload con un envelope nuevo y devuelve el envelope enviado. */
    public <T> EventEnvelope<T> publish(T payload, Object aggregateId) {
        EventEnvelope<T> event = envelope(payload, aggregateId, "test-" + UUID.randomUUID());
        send(event);
        return event;
    }

    public void send(EventEnvelope<?> event) {
        sendRaw(EventType.valueOf(event.eventType()).topic(), event.aggregateId(), toJson(event));
    }

    /** Publica un mensaje arbitrario (p. ej. JSON inválido para probar el DLT). */
    public void sendRaw(String topic, String key, String value) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Could not publish test event to " + topic, ex);
        }
    }

    public String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
