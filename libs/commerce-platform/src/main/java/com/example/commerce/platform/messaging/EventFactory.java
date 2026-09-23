package com.example.commerce.platform.messaging;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.EventType;
import com.example.commerce.platform.correlation.CorrelationId;

import java.time.Clock;
import java.util.UUID;

/**
 * Crea envelopes con metadata consistente. El correlation ID se toma del contexto actual (petición
 * HTTP o evento que se está procesando), así la saga completa comparte el mismo valor.
 */
public class EventFactory {

    private final Clock clock;
    private final String source;

    public EventFactory(Clock clock, String source) {
        this.clock = clock;
        this.source = source;
    }

    public <T> EventEnvelope<T> create(T payload, Object aggregateId) {
        EventType type = EventType.forPayload(payload);
        return new EventEnvelope<>(
                UUID.randomUUID(),
                type.name(),
                type.version(),
                clock.instant(),
                type.aggregateType(),
                String.valueOf(aggregateId),
                CorrelationId.currentOrNew(),
                source,
                payload);
    }
}
