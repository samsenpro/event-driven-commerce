package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record InventoryReleased(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId
) {
}
