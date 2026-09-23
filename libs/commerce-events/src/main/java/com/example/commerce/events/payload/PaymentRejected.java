package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.UUID;

public record PaymentRejected(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId,
        @NotNull UUID paymentId,
        @NotBlank String reason
) {
}
