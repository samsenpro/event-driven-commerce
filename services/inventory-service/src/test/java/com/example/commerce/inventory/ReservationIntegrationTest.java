package com.example.commerce.inventory;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.inventory.entity.ReservationStatus;
import com.example.commerce.inventory.service.ReservationService;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ReservationIntegrationTest extends InventoryIntegrationTest {

    @Autowired
    private IdempotentEventProcessor processor;

    @Autowired
    private ReservationService reservationService;

    @Test
    void orderCreatedReservesStockAndPublishesInventoryReserved() throws Exception {
        long keyboard = createProduct("10.00", 5);
        long mouse = createProduct("10.00", 5);
        long orderId = uniqueId();

        events.send(events.envelope(orderCreated(orderId, keyboard, 2, mouse, 1), orderId, "cid-reserve"));

        awaitReservation(orderId, ReservationStatus.RESERVED);
        assertThat(stock(keyboard).available()).isEqualTo(3);
        assertThat(stock(keyboard).reserved()).isEqualTo(2);
        JsonNode reserved = topics.body(topics.awaitRecord(Topics.INVENTORY_RESERVED, orderId));
        assertThat(reserved.get("correlationId").asText()).isEqualTo("cid-reserve");
        assertThat(reserved.get("payload").get("items")).hasSize(2);
    }

    @Test
    void insufficientStockReservesNothingAndPublishesFailure() throws Exception {
        long plenty = createProduct("10.00", 100);
        long scarce = createProduct("10.00", 1);
        long orderId = uniqueId();

        events.publish(orderCreated(orderId, plenty, 5, scarce, 2), orderId);

        awaitReservation(orderId, ReservationStatus.REJECTED);
        assertThat(stock(plenty).available()).isEqualTo(100);
        assertThat(stock(plenty).reserved()).isZero();
        assertThat(stock(scarce).available()).isEqualTo(1);
        JsonNode failed = topics.body(topics.awaitRecord(Topics.INVENTORY_FAILED, orderId));
        assertThat(failed.get("payload").get("unavailableProductIds").get(0).asLong()).isEqualTo(scarce);
    }

    @Test
    void cancellationReleasesReservedStock() throws Exception {
        long product = createProduct("10.00", 5);
        long orderId = uniqueId();
        events.publish(orderCreated(orderId, product, 4), orderId);
        awaitReservation(orderId, ReservationStatus.RESERVED);

        events.publish(new OrderCancelled(orderId, 99L, "PAYMENT_REJECTED"), orderId);

        awaitReservation(orderId, ReservationStatus.RELEASED);
        assertThat(stock(product).available()).isEqualTo(5);
        assertThat(stock(product).reserved()).isZero();
        assertThat(topics.awaitRecord(Topics.INVENTORY_RELEASED, orderId)).isNotNull();
    }

    @Test
    void confirmationCommitsTheReservedStock() throws Exception {
        long product = createProduct("10.00", 5);
        long orderId = uniqueId();
        events.publish(orderCreated(orderId, product, 2), orderId);
        awaitReservation(orderId, ReservationStatus.RESERVED);

        events.publish(new OrderConfirmed(orderId, 99L, new BigDecimal("20.00")), orderId);

        awaitReservation(orderId, ReservationStatus.COMMITTED);
        assertThat(stock(product).available()).isEqualTo(3);
        assertThat(stock(product).reserved()).isZero();
    }

    @Test
    void cancellationArrivingBeforeCreationPreventsTheLateReservation() throws Exception {
        long product = createProduct("10.00", 5);
        long orderId = uniqueId();

        events.publish(new OrderCancelled(orderId, 99L, "CUSTOMER_REQUEST"), orderId);
        awaitReservation(orderId, ReservationStatus.VOIDED);
        events.publish(orderCreated(orderId, product, 2), orderId);

        assertThat(topics.collect(Topics.INVENTORY_RESERVED, record -> String.valueOf(orderId).equals(record.key()),
                Duration.ofSeconds(3))).isEmpty();
        assertThat(stock(product).available()).isEqualTo(5);
    }

    @Test
    void duplicatedEventReservesStockOnlyOnce() throws Exception {
        long product = createProduct("10.00", 10);
        long orderId = uniqueId();
        EventEnvelope<OrderCreated> event = events.envelope(orderCreated(orderId, product, 3), orderId, "dup");

        events.send(event);
        events.send(event);
        awaitReservation(orderId, ReservationStatus.RESERVED);

        assertThat(topics.collect(Topics.INVENTORY_RESERVED, record -> String.valueOf(orderId).equals(record.key()),
                Duration.ofSeconds(3))).hasSize(1);
        assertThat(stock(product).reserved()).isEqualTo(3);
    }

    /**
     * El mismo evento procesado a la vez por dos hilos (p. ej. un reenvío durante un rebalanceo):
     * el INSERT ... ON CONFLICT DO NOTHING hace que solo uno aplique los efectos.
     */
    @Test
    void concurrentDeliveriesOfTheSameEventHaveASingleEffect() throws Exception {
        long product = createProduct("10.00", 10);
        long orderId = uniqueId();
        EventEnvelope<OrderCreated> event = events.envelope(orderCreated(orderId, product, 4), orderId, "race");
        CountDownLatch start = new CountDownLatch(1);
        Callable<Boolean> delivery = () -> {
            start.await();
            return processor.process("reserve-stock", event, reservationService::reserve);
        };

        List<Boolean> results;
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Boolean> first = executor.submit(delivery);
            Future<Boolean> second = executor.submit(delivery);
            start.countDown();
            results = List.of(first.get(), second.get());
        }

        assertThat(results).containsExactlyInAnyOrder(true, false);
        assertThat(stock(product).reserved()).isEqualTo(4);
        assertThat(stock(product).available()).isEqualTo(6);
    }

    /** Dos pedidos distintos compiten por las últimas unidades: el stock nunca queda negativo. */
    @Test
    void competingOrdersNeverOversell() throws Exception {
        long product = createProduct("10.00", 5);
        long firstOrder = uniqueId();
        long secondOrder = uniqueId();

        events.publish(orderCreated(firstOrder, product, 4), firstOrder);
        events.publish(orderCreated(secondOrder, product, 4), secondOrder);

        ReservationStatus first = awaitAnyFinalStatus(firstOrder);
        ReservationStatus second = awaitAnyFinalStatus(secondOrder);
        assertThat(List.of(first, second)).containsExactlyInAnyOrder(ReservationStatus.RESERVED, ReservationStatus.REJECTED);
        assertThat(stock(product).available()).isEqualTo(1);
        assertThat(stock(product).reserved()).isEqualTo(4);
    }

    private ReservationStatus awaitAnyFinalStatus(long orderId) {
        await().atMost(Duration.ofSeconds(20))
                .until(() -> reservationRepository.findByOrderId(orderId).isPresent());
        return reservationRepository.findByOrderId(orderId).orElseThrow().getStatus();
    }
}
