package com.example.commerce.platform.idempotency;

import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/**
 * Registro de eventos ya procesados ({@code processed_events}, clave primaria {@code (consumer, event_id)}).
 */
public class ProcessedEventStore {

    /**
     * Inserta el par (consumer, eventId) solo si no existe. Es atómico y seguro ante concurrencia:
     * si dos transacciones intentan registrar el mismo evento a la vez, la segunda espera al índice
     * único; cuando la primera hace commit, la segunda inserta 0 filas y sabe que es un duplicado.
     * Si la primera hace rollback, la segunda sí lo registra y procesa el evento.
     */
    private static final String INSERT_IF_ABSENT = """
            INSERT INTO processed_events (consumer, event_id, processed_at)
            VALUES (?, ?, ?)
            ON CONFLICT (consumer, event_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** @return {@code true} si el evento se registra ahora; {@code false} si ya estaba procesado */
    public boolean markProcessed(String consumer, UUID eventId, Instant now) {
        return jdbcTemplate.update(INSERT_IF_ABSENT, consumer, eventId, Timestamp.from(now)) == 1;
    }

    public long count(String consumer, UUID eventId) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE consumer = ? AND event_id = ?",
                Long.class, consumer, eventId);
        return count == null ? 0 : count;
    }
}
