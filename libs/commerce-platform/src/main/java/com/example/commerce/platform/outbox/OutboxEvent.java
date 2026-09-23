package com.example.commerce.platform.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * Fila de la tabla {@code outbox_events}.
 *
 * @param id            igual al {@code eventId} del envelope
 * @param position      secuencia de inserción; define el orden de publicación
 * @param topic         topic de destino
 * @param payload       envelope completo serializado en JSON (lo que se publica tal cual)
 * @param retryCount    intentos de publicación fallidos
 * @param nextAttemptAt no se reintenta antes de este instante (backoff)
 */
public record OutboxEvent(
        UUID id,
        long position,
        String aggregateType,
        String aggregateId,
        String eventType,
        String topic,
        String payload,
        String correlationId,
        OutboxStatus status,
        int retryCount,
        Instant createdAt,
        Instant nextAttemptAt,
        Instant publishedAt,
        String lastError
) {
}
