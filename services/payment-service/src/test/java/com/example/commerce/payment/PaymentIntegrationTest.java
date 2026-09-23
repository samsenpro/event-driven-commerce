package com.example.commerce.payment;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.PaymentRequested;
import com.example.commerce.payment.entity.Payment;
import com.example.commerce.payment.entity.PaymentStatus;
import com.example.commerce.payment.repository.PaymentRepository;
import com.example.commerce.testing.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class PaymentIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void paymentWithinLimitIsApproved() throws Exception {
        long orderId = uniqueId();

        events.send(events.envelope(new PaymentRequested(orderId, 7L, new BigDecimal("150.00")), orderId, "cid-pay"));

        assertThat(awaitPayment(orderId, PaymentStatus.APPROVED).getAmount()).isEqualByComparingTo("150.00");
        JsonNode approved = topics.body(topics.awaitRecord(Topics.PAYMENTS_APPROVED, orderId));
        assertThat(approved.get("correlationId").asText()).isEqualTo("cid-pay");
        mockMvc.perform(get("/api/v1/payments/{orderId}", orderId).header(HttpHeaders.AUTHORIZATION, adminToken()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPROVED"));
    }

    @Test
    void paymentAboveTheLimitIsRejectedWithoutRetrying() {
        long orderId = uniqueId();

        events.publish(new PaymentRequested(orderId, 7L, new BigDecimal("5000.00")), orderId);

        assertThat(awaitPayment(orderId, PaymentStatus.REJECTED).getReason()).contains("approval limit");
        assertThat(topics.body(topics.awaitRecord(Topics.PAYMENTS_REJECTED, orderId))
                .get("payload").get("reason").asText()).contains("approval limit");
    }

    @Test
    void cancellationAfterPaymentRefundsIt() {
        long orderId = uniqueId();
        events.publish(new PaymentRequested(orderId, 7L, new BigDecimal("80.00")), orderId);
        awaitPayment(orderId, PaymentStatus.APPROVED);

        events.publish(new OrderCancelled(orderId, 7L, "CUSTOMER_REQUEST"), orderId);

        awaitPayment(orderId, PaymentStatus.REFUNDED);
    }

    @Test
    void cancellationBeforeTheRequestPreventsTheLateCharge() {
        long orderId = uniqueId();

        events.publish(new OrderCancelled(orderId, 7L, "CUSTOMER_REQUEST"), orderId);
        awaitPayment(orderId, PaymentStatus.VOIDED);
        events.publish(new PaymentRequested(orderId, 7L, new BigDecimal("80.00")), orderId);

        assertThat(topics.collect(Topics.PAYMENTS_APPROVED, record -> String.valueOf(orderId).equals(record.key()),
                Duration.ofSeconds(3))).isEmpty();
        assertThat(paymentRepository.findByOrderId(orderId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
    }

    @Test
    void duplicatedRequestChargesOnlyOnce() {
        long orderId = uniqueId();
        EventEnvelope<PaymentRequested> request =
                events.envelope(new PaymentRequested(orderId, 7L, new BigDecimal("10.00")), orderId, "dup");

        events.send(request);
        events.send(request);
        awaitPayment(orderId, PaymentStatus.APPROVED);

        assertThat(topics.collect(Topics.PAYMENTS_APPROVED, record -> String.valueOf(orderId).equals(record.key()),
                Duration.ofSeconds(3))).hasSize(1);
    }

    @Test
    void onlyAdminsCanQueryPayments() throws Exception {
        mockMvc.perform(get("/api/v1/payments/{orderId}", 1).header(HttpHeaders.AUTHORIZATION, userToken(uniqueId())))
                .andExpect(status().isForbidden());
        mockMvc.perform(get("/api/v1/payments/{orderId}", Long.MAX_VALUE).header(HttpHeaders.AUTHORIZATION, adminToken()))
                .andExpect(status().isNotFound());
    }

    private Payment awaitPayment(long orderId, PaymentStatus expected) {
        await().atMost(Duration.ofSeconds(20)).until(() -> paymentRepository.findByOrderId(orderId)
                .map(Payment::getStatus).orElse(null) == expected);
        return paymentRepository.findByOrderId(orderId).orElseThrow();
    }
}
