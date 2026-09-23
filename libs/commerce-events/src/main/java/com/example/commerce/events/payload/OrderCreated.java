package com.example.commerce.events.payload;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.List;

public record OrderCreated(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId,
        @NotEmpty List<@NotNull @Valid OrderLine> items,
        @NotNull @Positive BigDecimal totalAmount
) {
}
