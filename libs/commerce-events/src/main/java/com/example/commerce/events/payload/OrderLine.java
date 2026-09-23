package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record OrderLine(
        @NotNull @Positive Long productId,
        @NotBlank String productName,
        @Positive int quantity,
        @NotNull @Positive BigDecimal unitPrice
) {
}
