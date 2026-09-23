package com.example.commerce.platform.messaging;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.EventType;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Convierte un mensaje de Kafka en un {@link EventEnvelope} tipado y valida el contrato:
 * <ul>
 *     <li>JSON legible y con la estructura del envelope,</li>
 *     <li>{@code eventType} igual al esperado en ese topic,</li>
 *     <li>{@code eventVersion} soportada por este consumidor,</li>
 *     <li>envelope y payload válidos según Bean Validation.</li>
 * </ul>
 * Cualquier fallo es permanente: {@link InvalidEventException} envía el mensaje al DLT sin reintentos.
 * Los campos desconocidos se ignoran, así que añadir campos opcionales es un cambio compatible.
 */
public class EventReader {

    private final ObjectMapper objectMapper;
    private final Validator validator;

    public EventReader(ObjectMapper objectMapper, Validator validator) {
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    public <T> EventEnvelope<T> read(ConsumerRecord<String, String> record, EventType expected) {
        EventEnvelope<T> event = parse(record, expected);
        if (!expected.name().equals(event.eventType())) {
            throw new InvalidEventException("Unexpected eventType " + event.eventType() + " on " + record.topic());
        }
        if (event.eventVersion() != expected.version()) {
            throw new InvalidEventException("Unsupported version " + event.eventVersion() + " of " + expected);
        }
        Set<ConstraintViolation<EventEnvelope<T>>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            throw new InvalidEventException("Invalid " + expected + ": " + describe(violations));
        }
        return event;
    }

    private <T> EventEnvelope<T> parse(ConsumerRecord<String, String> record, EventType expected) {
        if (record.value() == null || record.value().isBlank()) {
            throw new InvalidEventException("Empty message on " + record.topic());
        }
        JavaType type = objectMapper.getTypeFactory()
                .constructParametricType(EventEnvelope.class, expected.payloadType());
        try {
            return objectMapper.readerFor(type)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .readValue(record.value());
        } catch (IOException | IllegalArgumentException ex) {
            throw new InvalidEventException("Unreadable " + expected + " on " + record.topic(), ex);
        }
    }

    private static String describe(Set<? extends ConstraintViolation<?>> violations) {
        return violations.stream()
                .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                .sorted()
                .collect(Collectors.joining(", "));
    }
}
