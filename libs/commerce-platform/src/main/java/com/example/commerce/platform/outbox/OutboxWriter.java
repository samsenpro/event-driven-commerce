package com.example.commerce.platform.outbox;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.EventType;
import com.example.commerce.platform.messaging.EventFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;

/**
 * Registra eventos en el outbox <b>dentro de la transacción de negocio</b>. Nunca publica en Kafka
 * directamente: el evento solo existirá si la transacción hace commit, y el {@link OutboxPublisher}
 * lo publicará después aunque Kafka no esté disponible en este momento.
 */
public class OutboxWriter {

    private final OutboxStore store;
    private final EventFactory eventFactory;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OutboxWriter(OutboxStore store, EventFactory eventFactory, ObjectMapper objectMapper, Clock clock) {
        this.store = store;
        this.eventFactory = eventFactory;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /** Crea el envelope del payload y lo guarda en el outbox. */
    public <T> EventEnvelope<T> publish(T payload, Object aggregateId) {
        EventEnvelope<T> event = eventFactory.create(payload, aggregateId);
        append(event);
        return event;
    }

    public void append(EventEnvelope<?> event) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox events must be written inside the business transaction");
        }
        EventType type = EventType.valueOf(event.eventType());
        store.insert(event.eventId(), event.aggregateType(), event.aggregateId(), event.eventType(), type.topic(),
                serialize(event), event.correlationId(), clock.instant());
    }

    private String serialize(EventEnvelope<?> event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Cannot serialize event " + event.eventType(), ex);
        }
    }
}
