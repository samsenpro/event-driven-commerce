package com.example.commerce.platform.kafka;

/**
 * Cabeceras que acompañan a cada mensaje. Duplican metadata del envelope para poder filtrar y
 * depurar (p. ej. en Kafka UI) sin deserializar el cuerpo.
 */
public final class KafkaHeaders {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String CORRELATION_ID = "correlationId";

    private KafkaHeaders() {
    }
}
