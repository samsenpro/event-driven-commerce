package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentApproved(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId,
        @NotNull UUID paymentId,
        @NotNull @Positive BigDecimal amount
) {
}
