package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record StockLine(
        @NotNull @Positive Long productId,
        @Positive int quantity
) {
}
