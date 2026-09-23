package com.example.commerce.platform.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @param enabled        activa el publicador en los servicios que producen eventos
 * @param pollInterval   frecuencia de consulta de eventos pendientes
 * @param batchSize      eventos por lote
 * @param maxAttempts    intentos de publicación antes de marcar FAILED
 * @param initialBackoff espera tras el primer fallo; se duplica en cada intento
 * @param sendTimeout    tiempo máximo esperando el ack de Kafka
 * @param retention      tiempo que se conservan los eventos PUBLISHED para auditoría
 */
@ConfigurationProperties(prefix = "commerce.outbox")
public record OutboxProperties(
        boolean enabled,
        Duration pollInterval,
        int batchSize,
        int maxAttempts,
        Duration initialBackoff,
        Duration sendTimeout,
        Duration retention
) {

    public OutboxProperties {
        pollInterval = pollInterval == null ? Duration.ofMillis(500) : pollInterval;
        batchSize = batchSize <= 0 ? 50 : batchSize;
        maxAttempts = maxAttempts <= 0 ? 3 : maxAttempts;
        initialBackoff = initialBackoff == null ? Duration.ofSeconds(2) : initialBackoff;
        sendTimeout = sendTimeout == null ? Duration.ofSeconds(5) : sendTimeout;
        retention = retention == null ? Duration.ofDays(7) : retention;
    }

    /** Backoff exponencial: initialBackoff * 2^(intentos fallidos - 1). */
    public Duration backoffAfter(int failedAttempts) {
        return initialBackoff.multipliedBy(1L << Math.max(0, failedAttempts - 1));
    }
}
