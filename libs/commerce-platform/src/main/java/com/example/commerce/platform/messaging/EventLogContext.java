package com.example.commerce.platform.messaging;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.platform.correlation.CorrelationId;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

/**
 * Pone en el MDC los datos del evento que se está procesando (correlationId, eventId, eventType,
 * aggregateId) para que aparezcan en todos los logs, y los retira al terminar.
 * Los eventos que se publiquen durante el procesamiento heredan el mismo correlation ID.
 */
public final class EventLogContext implements AutoCloseable {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String AGGREGATE_ID = "aggregateId";

    private final Map<String, String> previous = new HashMap<>();

    private EventLogContext(EventEnvelope<?> event) {
        put(CorrelationId.MDC_KEY, event.correlationId());
        put(EVENT_ID, String.valueOf(event.eventId()));
        put(EVENT_TYPE, event.eventType());
        put(AGGREGATE_ID, event.aggregateId());
    }

    public static EventLogContext open(EventEnvelope<?> event) {
        return new EventLogContext(event);
    }

    private void put(String key, String value) {
        previous.put(key, MDC.get(key));
        MDC.put(key, value);
    }

    @Override
    public void close() {
        previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }
}
