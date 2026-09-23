package com.example.commerce.events.payload;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Estado completo del producto (event-carried state transfer): los consumidores mantienen una
 * réplica local del catálogo sin consultar al servicio de inventario.
 *
 * @param updatedAt permite descartar eventos más antiguos que la réplica ya aplicada
 */
public record ProductChanged(
        @NotNull @Positive Long productId,
        @NotBlank String sku,
        @NotBlank String name,
        @NotNull @Positive BigDecimal price,
        boolean active,
        @NotNull Instant updatedAt
) {
}
