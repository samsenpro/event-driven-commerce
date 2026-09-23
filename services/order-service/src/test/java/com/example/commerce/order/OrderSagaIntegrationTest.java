package com.example.commerce.order;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.InventoryReservationFailed;
import com.example.commerce.events.payload.InventoryReserved;
import com.example.commerce.events.payload.PaymentApproved;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.ShipmentCreated;
import com.example.commerce.events.payload.StockLine;
import com.example.commerce.order.entity.CancellationReason;
import com.example.commerce.order.entity.Order;
import com.example.commerce.order.entity.OrderStatus;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * La saga vista desde el order-service: se simulan con Kafka real los eventos que publicarían
 * inventory, payment y shipping, y se verifica cómo reacciona el pedido y qué publica.
 */
class OrderSagaIntegrationTest extends OrderServiceIntegrationTest {

    private static final Duration QUIET_PERIOD = Duration.ofSeconds(3);

    @Test
    void happyPathReservesPaysConfirmsAndShipsKeepingTheCorrelationId() {
        long userId = uniqueId();
        long orderId = newOrder(userId, "saga-ok");

        events.send(fromInventory(new InventoryReserved(orderId, userId, List.of(new StockLine(1L, 1))), orderId, "saga-ok"));
        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);

        ConsumerRecord<String, String> paymentRequest = topics.awaitRecord(Topics.PAYMENTS_REQUESTED, orderId);
        JsonNode request = topics.body(paymentRequest);
        assertThat(request.get("correlationId").asText()).isEqualTo("saga-ok");
        assertThat(payloadOf(request).get("amount").decimalValue()).isEqualByComparingTo("25.00");

        events.send(fromInventory(new PaymentApproved(orderId, userId, UUID.randomUUID(), new BigDecimal("25.00")),
                orderId, "saga-ok"));
        awaitStatus(orderId, OrderStatus.CONFIRMED);
        assertThat(topics.body(topics.awaitRecord(Topics.ORDERS_CONFIRMED, orderId)).get("correlationId").asText())
                .isEqualTo("saga-ok");

        events.send(fromInventory(new ShipmentCreated(orderId, userId, UUID.randomUUID(), "TRK-1"), orderId, "saga-ok"));
        awaitStatus(orderId, OrderStatus.SHIPPED);
    }

    @Test
    void rejectedPaymentCancelsTheOrderSoInventoryCanCompensate() {
        long userId = uniqueId();
        long orderId = newOrder(userId, "saga-rejected");
        events.send(fromInventory(new InventoryReserved(orderId, userId, List.of(new StockLine(1L, 1))), orderId,
                "saga-rejected"));
        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);

        events.send(fromInventory(new PaymentRejected(orderId, userId, UUID.randomUUID(), "Insufficient funds"),
                orderId, "saga-rejected"));

        Order order = awaitStatus(orderId, OrderStatus.CANCELLED);
        assertThat(order.getCancellationReason()).isEqualTo(CancellationReason.PAYMENT_REJECTED);
        JsonNode cancelled = topics.body(topics.awaitRecord(Topics.ORDERS_CANCELLED, orderId));
        assertThat(payloadOf(cancelled).get("reason").asText()).isEqualTo("PAYMENT_REJECTED");
        assertThat(cancelled.get("correlationId").asText()).isEqualTo("saga-rejected");
    }

    @Test
    void outOfStockCancelsTheOrder() {
        long userId = uniqueId();
        long orderId = newOrder(userId, "saga-no-stock");

        events.send(fromInventory(new InventoryReservationFailed(orderId, userId, "Insufficient stock", List.of(1L)),
                orderId, "saga-no-stock"));

        assertThat(awaitStatus(orderId, OrderStatus.CANCELLED).getCancellationReason())
                .isEqualTo(CancellationReason.OUT_OF_STOCK);
    }

    @Test
    void duplicatedEventIsProcessedOnlyOnce() {
        long userId = uniqueId();
        long orderId = newOrder(userId, "saga-dup");
        EventEnvelope<InventoryReserved> reserved =
                fromInventory(new InventoryReserved(orderId, userId, List.of(new StockLine(1L, 1))), orderId, "saga-dup");

        events.send(reserved);
        events.send(reserved);
        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);

        List<ConsumerRecord<String, String>> paymentRequests = topics.collect(Topics.PAYMENTS_REQUESTED,
                record -> String.valueOf(orderId).equals(record.key()), QUIET_PERIOD);
        assertThat(paymentRequests).hasSize(1);
    }

    @Test
    void paymentApprovedAfterCustomerCancellationIsIgnored() throws Exception {
        long userId = uniqueId();
        long orderId = newOrder(userId, "saga-late");
        events.send(fromInventory(new InventoryReserved(orderId, userId, List.of(new StockLine(1L, 1))), orderId, "saga-late"));
        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);

        mockMvc.perform(patch("/api/v1/orders/{id}/cancel", orderId).header(HttpHeaders.AUTHORIZATION, userToken(userId)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        events.send(fromInventory(new PaymentApproved(orderId, userId, UUID.randomUUID(), new BigDecimal("25.00")),
                orderId, "saga-late"));

        assertThat(topics.collect(Topics.ORDERS_CONFIRMED, record -> String.valueOf(orderId).equals(record.key()),
                QUIET_PERIOD)).isEmpty();
        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(topics.awaitRecord(Topics.ORDERS_CANCELLED, orderId)).isNotNull();
    }

    @Test
    void shippedOrdersCannotBeCancelled() throws Exception {
        long userId = uniqueId();
        long orderId = newOrder(userId, "saga-shipped");
        events.send(fromInventory(new InventoryReserved(orderId, userId, List.of(new StockLine(1L, 1))), orderId, "c"));
        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);
        events.send(fromInventory(new PaymentApproved(orderId, userId, UUID.randomUUID(), BigDecimal.TEN), orderId, "c"));
        awaitStatus(orderId, OrderStatus.CONFIRMED);
        events.send(fromInventory(new ShipmentCreated(orderId, userId, UUID.randomUUID(), "TRK"), orderId, "c"));
        awaitStatus(orderId, OrderStatus.SHIPPED);

        mockMvc.perform(patch("/api/v1/orders/{id}/cancel", orderId).header(HttpHeaders.AUTHORIZATION, userToken(userId)))
                .andExpect(status().isUnprocessableEntity());
    }

    private long newOrder(long userId, String correlationId) {
        try {
            return placeOrder(userId, correlationId, catalogProduct("25.00", true), 1);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private <T> EventEnvelope<T> fromInventory(T payload, long orderId, String correlationId) {
        return events.envelope(payload, orderId, correlationId);
    }
}
