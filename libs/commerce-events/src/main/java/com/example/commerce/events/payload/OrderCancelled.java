package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * @param reason motivo de negocio: CUSTOMER_REQUEST, OUT_OF_STOCK, PAYMENT_REJECTED...
 */
public record OrderCancelled(
        @NotNull @Positive Long orderId,
        @NotNull @Positive Long userId,
        @NotBlank String reason
) {
}
