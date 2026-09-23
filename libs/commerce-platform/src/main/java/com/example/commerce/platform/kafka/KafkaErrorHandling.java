package com.example.commerce.platform.kafka;

import com.example.commerce.events.Topics;
import com.example.commerce.platform.messaging.NonRetryableEventException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * Estrategia de errores de todos los consumidores:
 * <pre>
 * error transitorio (BD caída, timeout, fallo temporal de un tercero)
 *     → reintento con backoff exponencial (maxRetries veces) → si persiste, DLT
 * error permanente (evento inválido, esquema desconocido, regla imposible de cumplir)
 *     → DLT inmediatamente, sin reintentos
 * </pre>
 * El DLT es {@code <topic>.DLT} y el mensaje conserva su partición, clave y cabeceras, más las
 * cabeceras de diagnóstico que añade Spring Kafka (excepción, topic, offset originales).
 */
public final class KafkaErrorHandling {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandling.class);

    private KafkaErrorHandling() {
    }

    public static DefaultErrorHandler errorHandler(KafkaOperations<?, ?> template,
                                                   CommerceKafkaProperties.Retry retry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(Topics.deadLetterOf(record.topic()), record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            logDeadLetter(record, ex);
            recoverer.accept(record, ex);
        }, backOff);
        handler.addNotRetryableExceptions(NonRetryableEventException.class, DeserializationException.class);
        handler.setRetryListeners((record, ex, attempt) -> log.warn(
                "Retrying event topic={} partition={} offset={} key={} attempt={} cause={}",
                record.topic(), record.partition(), record.offset(), record.key(), attempt, rootMessage(ex)));
        return handler;
    }

    private static void logDeadLetter(ConsumerRecord<?, ?> record, Exception ex) {
        log.error("Sending event to DLT topic={} partition={} offset={} key={} cause={}",
                Topics.deadLetterOf(record.topic()), record.partition(), record.offset(), record.key(),
                rootMessage(ex));
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
