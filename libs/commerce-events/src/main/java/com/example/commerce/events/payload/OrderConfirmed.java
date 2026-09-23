package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record OrderConfirmed(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId,
        @NotNull @Positive BigDecimal totalAmount
) {
}
