package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.List;

/**
 * @param unavailableProductIds productos sin stock suficiente, inexistentes o inactivos
 */
public record InventoryReservationFailed(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId,
        @NotBlank String reason,
        @NotNull List<Long> unavailableProductIds
) {
}
