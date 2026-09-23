package com.example.commerce.order.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.InventoryReserved;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.events.payload.PaymentApproved;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.PaymentRequested;
import com.example.commerce.events.payload.StockLine;
import com.example.commerce.order.entity.CancellationReason;
import com.example.commerce.order.entity.Order;
import com.example.commerce.order.entity.OrderItem;
import com.example.commerce.order.entity.OrderStatus;
import com.example.commerce.order.repository.OrderRepository;
import com.example.commerce.platform.messaging.NonRetryableEventException;
import com.example.commerce.platform.outbox.OutboxWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderSagaServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-23T10:00:00Z");
    private static final long ORDER_ID = 42L;

    @Mock
    private OrderRepository orderRepository;
    @Mock
    private OutboxWriter outbox;

    private OrderSagaService saga;
    private Order order;

    @BeforeEach
    void setUp() {
        saga = new OrderSagaService(orderRepository, outbox, Clock.fixed(NOW, ZoneOffset.UTC));
        order = Order.place(7L, List.of(new OrderItem(1L, "Keyboard", 2, new BigDecimal("50.00"))), NOW);
        ReflectionTestUtils.setField(order, "id", ORDER_ID);
    }

    @Test
    void inventoryReservedRequestsPaymentForTheOrderTotal() {
        when(orderRepository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        saga.onInventoryReserved(event(new InventoryReserved(ORDER_ID, 7L, List.of(new StockLine(1L, 2)))));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
        ArgumentCaptor<PaymentRequested> request = ArgumentCaptor.forClass(PaymentRequested.class);
        verify(outbox).publish(request.capture(), eq(ORDER_ID));
        assertThat(request.getValue().amount()).isEqualByComparingTo("100.00");
    }

    @Test
    void paymentApprovedConfirmsTheOrder() {
        order.markInventoryReserved(NOW);
        when(orderRepository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        saga.onPaymentApproved(event(new PaymentApproved(ORDER_ID, 7L, UUID.randomUUID(), new BigDecimal("100.00"))));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        verify(outbox).publish(any(OrderConfirmed.class), eq(ORDER_ID));
    }

    @Test
    void paymentRejectedCancelsTheOrderAsCompensation() {
        order.markInventoryReserved(NOW);
        when(orderRepository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        saga.onPaymentRejected(event(new PaymentRejected(ORDER_ID, 7L, UUID.randomUUID(), "Insufficient funds")));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancellationReason()).isEqualTo(CancellationReason.PAYMENT_REJECTED);
        ArgumentCaptor<OrderCancelled> cancelled = ArgumentCaptor.forClass(OrderCancelled.class);
        verify(outbox).publish(cancelled.capture(), eq(ORDER_ID));
        assertThat(cancelled.getValue().reason()).isEqualTo("PAYMENT_REJECTED");
    }

    @Test
    void lateEventsForACancelledOrderAreIgnored() {
        order.cancel(CancellationReason.CUSTOMER_REQUEST, NOW);
        when(orderRepository.findById(ORDER_ID)).thenReturn(Optional.of(order));

        saga.onPaymentApproved(event(new PaymentApproved(ORDER_ID, 7L, UUID.randomUUID(), BigDecimal.TEN)));
        saga.onInventoryReserved(event(new InventoryReserved(ORDER_ID, 7L, List.of(new StockLine(1L, 2)))));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(outbox, never()).publish(any(), any());
    }

    @Test
    void eventForAnUnknownOrderIsAPermanentError() {
        when(orderRepository.findById(ORDER_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> saga.onPaymentApproved(
                event(new PaymentApproved(ORDER_ID, 7L, UUID.randomUUID(), BigDecimal.TEN))))
                .isInstanceOf(NonRetryableEventException.class);
    }

    private static <T> EventEnvelope<T> event(T payload) {
        return new EventEnvelope<>(UUID.randomUUID(), "TEST", 1, NOW, "Order", String.valueOf(ORDER_ID), "cid",
                "test", payload);
    }
}
