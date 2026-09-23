package com.example.commerce.order.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * El cliente solo indica productos y cantidades; los precios salen de la réplica del catálogo.
 */
public record CreateOrderRequest(
        @NotEmpty @Size(max = MAX_ITEMS)
        List<@NotNull @Valid Item> items
) {

    public static final int MAX_ITEMS = 50;
    public static final int MAX_QUANTITY = 1_000;

    public record Item(
            @Schema(example = "1") @NotNull @Positive Long productId,
            @Schema(example = "2") @NotNull @Positive @Max(MAX_QUANTITY) Integer quantity
    ) {
    }
}
