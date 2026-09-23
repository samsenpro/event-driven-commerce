package com.example.commerce.platform.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Acceso a {@code outbox_events} con JDBC. Participa en la transacción JPA del servicio
 * (misma conexión), por lo que el insert del evento se confirma o se deshace junto con los datos
 * de negocio.
 */
public class OutboxStore {

    private static final String INSERT = """
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, topic, payload,
                                       correlation_id, status, retry_count, created_at, next_attempt_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)
            """;

    /**
     * Siguiente lote publicable. Solo se toma la <b>cabeza</b> de cada agregado: un evento no sale
     * mientras quede otro anterior del mismo agregado sin publicar (PENDING o FAILED). Así se
     * respeta el orden por agregado aunque haya reintentos o varias instancias.
     * {@code SKIP LOCKED} permite que varias instancias del servicio publiquen en paralelo sin
     * procesar dos veces la misma fila.
     */
    private static final String LOCK_NEXT_BATCH = """
            SELECT * FROM outbox_events o
             WHERE o.status = 'PENDING'
               AND o.next_attempt_at <= ?
               AND NOT EXISTS (SELECT 1 FROM outbox_events earlier
                                WHERE earlier.aggregate_type = o.aggregate_type
                                  AND earlier.aggregate_id = o.aggregate_id
                                  AND earlier.status IN ('PENDING', 'FAILED')
                                  AND earlier.position < o.position)
             ORDER BY o.position
             LIMIT ?
             FOR UPDATE SKIP LOCKED
            """;

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getObject("id", UUID.class),
            rs.getLong("position"),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getString("event_type"),
            rs.getString("topic"),
            rs.getString("payload"),
            rs.getString("correlation_id"),
            OutboxStatus.valueOf(rs.getString("status")),
            rs.getInt("retry_count"),
            toInstant(rs.getTimestamp("created_at")),
            toInstant(rs.getTimestamp("next_attempt_at")),
            toInstant(rs.getTimestamp("published_at")),
            rs.getString("last_error"));

    private final JdbcTemplate jdbcTemplate;

    public OutboxStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(UUID id, String aggregateType, String aggregateId, String eventType, String topic,
                       String payload, String correlationId, Instant now) {
        jdbcTemplate.update(INSERT, id, aggregateType, aggregateId, eventType, topic, payload, correlationId,
                Timestamp.from(now), Timestamp.from(now));
    }

    public List<OutboxEvent> lockNextBatch(int batchSize, Instant now) {
        return jdbcTemplate.query(LOCK_NEXT_BATCH, ROW_MAPPER, Timestamp.from(now), batchSize);
    }

    public void markPublished(UUID id, Instant now) {
        jdbcTemplate.update("UPDATE outbox_events SET status = 'PUBLISHED', published_at = ?, last_error = NULL "
                + "WHERE id = ?", Timestamp.from(now), id);
    }

    public void markAttemptFailed(UUID id, int retryCount, OutboxStatus status, Instant nextAttemptAt, String error) {
        jdbcTemplate.update("UPDATE outbox_events SET retry_count = ?, status = ?, next_attempt_at = ?, last_error = ? "
                + "WHERE id = ?", retryCount, status.name(), Timestamp.from(nextAttemptAt), error, id);
    }

    public long countByStatus(OutboxStatus status) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE status = ?", Long.class, status.name());
        return count == null ? 0 : count;
    }

    public List<OutboxEvent> findByAggregate(String aggregateType, String aggregateId) {
        return jdbcTemplate.query("SELECT * FROM outbox_events WHERE aggregate_type = ? AND aggregate_id = ? "
                + "ORDER BY position", ROW_MAPPER, aggregateType, aggregateId);
    }

    public int deletePublishedBefore(Instant threshold) {
        return jdbcTemplate.update("DELETE FROM outbox_events WHERE status = 'PUBLISHED' AND published_at < ?",
                Timestamp.from(threshold));
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
