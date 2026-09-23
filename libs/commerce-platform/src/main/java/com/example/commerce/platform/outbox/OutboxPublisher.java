package com.example.commerce.platform.outbox;

import com.example.commerce.platform.correlation.CorrelationId;
import com.example.commerce.platform.kafka.KafkaHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica en Kafka los eventos pendientes del outbox.
 * <ul>
 *     <li>Clave del mensaje = {@code aggregateId} (p. ej. orderId): todos los eventos de un pedido
 *     van a la misma partición y se consumen en orden.</li>
 *     <li>Entrega <b>at-least-once</b>: si el servicio cae entre el ack de Kafka y el UPDATE a
 *     PUBLISHED, el evento se vuelve a publicar. Los consumidores son idempotentes por {@code eventId}.</li>
 *     <li>Si Kafka falla, el evento sigue PENDING con backoff exponencial; al agotar
 *     {@code maxAttempts} pasa a FAILED y queda para revisión (no se pierde).</li>
 * </ul>
 */
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int MAX_ERROR_LENGTH = 500;

    private final OutboxStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxPublisher(OutboxStore store, KafkaTemplate<String, String> kafkaTemplate,
                           TransactionTemplate transactionTemplate, OutboxProperties properties, Clock clock) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Repite mientras encuentre eventos: al publicar la cabeza de un agregado, su siguiente evento pasa
     * a ser elegible en la misma ejecución. Termina siempre, porque los eventos publicados o en espera
     * de reintento (backoff) ya no se seleccionan.
     */
    @Scheduled(fixedDelayString = "${commerce.outbox.poll-interval:500ms}")
    public void publishPendingEvents() {
        Integer processed;
        do {
            processed = transactionTemplate.execute(status -> publishBatch());
        } while (processed != null && processed > 0);
    }

    /**
     * Toma un lote con bloqueo de fila, envía todos los mensajes en paralelo y espera cada ack.
     * El lote completo se actualiza en la misma transacción que mantiene los bloqueos.
     */
    int publishBatch() {
        List<OutboxEvent> batch = store.lockNextBatch(properties.batchSize(), clock.instant());
        if (batch.isEmpty()) {
            return 0;
        }
        List<CompletableFuture<SendResult<String, String>>> sends = batch.stream().map(this::send).toList();
        for (int i = 0; i < batch.size(); i++) {
            awaitAndRecord(batch.get(i), sends.get(i));
        }
        return batch.size();
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.topic(), event.aggregateId(), event.payload());
        header(record, KafkaHeaders.EVENT_ID, event.id().toString());
        header(record, KafkaHeaders.EVENT_TYPE, event.eventType());
        header(record, KafkaHeaders.CORRELATION_ID, event.correlationId());
        try {
            return kafkaTemplate.send(record);
        } catch (RuntimeException ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private void awaitAndRecord(OutboxEvent event, CompletableFuture<SendResult<String, String>> send) {
        MDC.put(CorrelationId.MDC_KEY, event.correlationId());
        try {
            SendResult<String, String> result = send.get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            store.markPublished(event.id(), clock.instant());
            log.info("Outbox event published event={} eventId={} aggregateId={} topic={} partition={} offset={}",
                    event.eventType(), event.id(), event.aggregateId(), event.topic(),
                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            registerFailure(event, ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            registerFailure(event, ex);
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }

    private void registerFailure(OutboxEvent event, Exception ex) {
        int attempts = event.retryCount() + 1;
        Instant now = clock.instant();
        String error = describe(ex);
        if (attempts >= properties.maxAttempts()) {
            store.markAttemptFailed(event.id(), attempts, OutboxStatus.FAILED, now, error);
            log.error("Outbox event FAILED after {} attempts event={} eventId={} aggregateId={} cause={}",
                    attempts, event.eventType(), event.id(), event.aggregateId(), error);
        } else {
            Instant nextAttempt = now.plus(properties.backoffAfter(attempts));
            store.markAttemptFailed(event.id(), attempts, OutboxStatus.PENDING, nextAttempt, error);
            log.warn("Outbox publish failed (attempt {}/{}), retrying at {} event={} eventId={} cause={}",
                    attempts, properties.maxAttempts(), nextAttempt, event.eventType(), event.id(), error);
        }
    }

    private static void header(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String describe(Exception ex) {
        Throwable root = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
        String message = root.getClass().getSimpleName() + ": " + root.getMessage();
        return message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;
    }
}
