package com.example.commerce.order.entity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private static final Instant NOW = Instant.parse("2026-09-23T10:00:00Z");

    @Test
    void totalIsTheSumOfTheSubtotals() {
        Order order = Order.place(1L, List.of(
                new OrderItem(1L, "Keyboard", 2, new BigDecimal("89.90")),
                new OrderItem(2L, "Mouse", 1, new BigDecimal("19.99"))), NOW);

        assertThat(order.getTotalAmount()).isEqualByComparingTo("199.79");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void followsTheHappyPath() {
        Order order = Order.place(1L, List.of(new OrderItem(1L, "Keyboard", 1, BigDecimal.TEN)), NOW);

        order.markInventoryReserved(NOW);
        order.confirm(NOW);
        order.markShipped(NOW);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.SHIPPED);
    }

    @Test
    void cannotSkipSteps() {
        Order order = Order.place(1L, List.of(new OrderItem(1L, "Keyboard", 1, BigDecimal.TEN)), NOW);

        assertThatThrownBy(() -> order.confirm(NOW)).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @CsvSource({
            "PENDING,CANCELLED,true",
            "INVENTORY_RESERVED,CANCELLED,true",
            "CONFIRMED,CANCELLED,true",
            "SHIPPED,CANCELLED,false",
            "CANCELLED,CONFIRMED,false",
            "PENDING,CONFIRMED,false"
    })
    void transitions(OrderStatus from, OrderStatus to, boolean allowed) {
        assertThat(from.canTransitionTo(to)).isEqualTo(allowed);
    }
}
