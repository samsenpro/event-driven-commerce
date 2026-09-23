package com.example.commerce.platform.idempotency;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.EventType;
import com.example.commerce.platform.messaging.EventLogContext;
import com.example.commerce.platform.messaging.EventReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.function.Consumer;

/**
 * Procesa un evento exactamente una vez por consumidor, en una única transacción local:
 * <pre>
 * BEGIN
 *   INSERT processed_events (consumer, eventId) ON CONFLICT DO NOTHING
 *     ├── 0 filas → duplicado: se ignora
 *     └── 1 fila  → handler: cambios de negocio + eventos de salida en el outbox
 * COMMIT
 * </pre>
 * Si el handler falla, el ROLLBACK deshace también el registro del evento, así que el reintento
 * de Kafka lo vuelve a procesar. Registro, efectos y eventos de salida son atómicos.
 */
public class IdempotentEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventProcessor.class);

    private final EventReader eventReader;
    private final ProcessedEventStore store;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final String serviceName;

    public IdempotentEventProcessor(EventReader eventReader, ProcessedEventStore store,
                                    TransactionTemplate transactionTemplate, Clock clock, String serviceName) {
        this.eventReader = eventReader;
        this.store = store;
        this.transactionTemplate = transactionTemplate;
        this.clock = clock;
        this.serviceName = serviceName;
    }

    /**
     * Lee y valida el mensaje ({@link EventReader}) y lo procesa una sola vez. Un mensaje inválido lanza
     * {@code InvalidEventException} y va al DLT.
     */
    public <T> boolean handle(ConsumerRecord<String, String> record, EventType type, String handlerName,
                             Consumer<EventEnvelope<T>> handler) {
        EventEnvelope<T> event = eventReader.read(record, type);
        return process(handlerName, event, handler);
    }

    /**
     * @param handlerName nombre del consumidor dentro del servicio; un mismo servicio puede procesar
     *                    el mismo evento en varios handlers independientes
     * @return {@code true} si se procesó; {@code false} si era un duplicado
     */
    public <T> boolean process(String handlerName, EventEnvelope<T> event, Consumer<EventEnvelope<T>> handler) {
        String consumer = serviceName + "." + handlerName;
        try (EventLogContext ignored = EventLogContext.open(event)) {
            Boolean processed = transactionTemplate.execute(status -> {
                if (!store.markProcessed(consumer, event.eventId(), clock.instant())) {
                    return false;
                }
                handler.accept(event);
                return true;
            });
            if (Boolean.TRUE.equals(processed)) {
                log.info("Event processed consumer={} event={} eventId={} aggregateId={}",
                        consumer, event.eventType(), event.eventId(), event.aggregateId());
                return true;
            }
            log.info("Duplicate event ignored consumer={} event={} eventId={}",
                    consumer, event.eventType(), event.eventId());
            return false;
        }
    }
}
