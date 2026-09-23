package com.example.commerce.payment;

import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.PaymentRequested;
import com.example.commerce.payment.processor.PaymentGatewayUnavailableException;
import com.example.commerce.payment.processor.PaymentProcessor;
import com.example.commerce.payment.processor.PaymentProcessor.PaymentResult;
import com.example.commerce.payment.repository.PaymentRepository;
import com.example.commerce.testing.AbstractIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Errores transitorios de la pasarela: el consumidor reintenta con backoff (3 reintentos) y, si
 * el fallo persiste, envía el evento a {@code payments.requested.DLT}.
 */
class PaymentRetryIntegrationTest extends AbstractIntegrationTest {

    @MockitoBean
    private PaymentProcessor paymentProcessor;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void persistentTransientFailureIsRetriedThreeTimesAndThenSentToTheDeadLetterTopic() {
        long orderId = uniqueId();
        when(paymentProcessor.charge(eq(orderId), any(), any()))
                .thenThrow(new PaymentGatewayUnavailableException("gateway down"));

        events.publish(new PaymentRequested(orderId, 7L, new BigDecimal("20.00")), orderId);

        ConsumerRecord<String, String> dead =
                topics.awaitRecord(Topics.deadLetterOf(Topics.PAYMENTS_REQUESTED), orderId);
        // 1 intento + 3 reintentos
        verify(paymentProcessor, times(4)).charge(eq(orderId), any(), any());
        assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(),
                StandardCharsets.UTF_8)).endsWith("PaymentGatewayUnavailableException");
        // Cada intento hizo rollback: no quedó ningún pago a medias
        assertThat(paymentRepository.findByOrderId(orderId)).isEmpty();
    }

    @Test
    void transientFailureFollowedBySuccessIsApprovedAfterRetrying() {
        long orderId = uniqueId();
        when(paymentProcessor.charge(eq(orderId), any(), any()))
                .thenThrow(new PaymentGatewayUnavailableException("blip"))
                .thenReturn(PaymentResult.approvedPayment());

        events.publish(new PaymentRequested(orderId, 7L, new BigDecimal("20.00")), orderId);

        assertThat(topics.awaitRecord(Topics.PAYMENTS_APPROVED, orderId)).isNotNull();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                verify(paymentProcessor, times(2)).charge(eq(orderId), any(), any()));
        assertThat(topics.collect(Topics.deadLetterOf(Topics.PAYMENTS_REQUESTED),
                record -> String.valueOf(orderId).equals(record.key()), Duration.ofSeconds(2))).isEmpty();
    }
}
