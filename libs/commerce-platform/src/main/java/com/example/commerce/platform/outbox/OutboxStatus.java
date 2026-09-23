package com.example.commerce.platform.outbox;

public enum OutboxStatus {
    /** Pendiente de publicar (incluye los que esperan un reintento). */
    PENDING,
    /** Confirmado por Kafka. Se conserva para auditoría hasta que expira la retención. */
    PUBLISHED,
    /** Agotó los intentos. Requiere intervención; bloquea los eventos posteriores del mismo agregado. */
    FAILED
}
