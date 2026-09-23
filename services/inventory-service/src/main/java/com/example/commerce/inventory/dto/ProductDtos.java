package com.example.commerce.inventory.dto;

import com.example.commerce.inventory.entity.Product;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * DTOs de productos e inventario.
 */
public final class ProductDtos {

    public static final int MAX_STOCK_ADJUSTMENT = 1_000_000;

    private ProductDtos() {
    }

    public record CreateProductRequest(
            @Schema(example = "KB-001") @NotBlank
            @Pattern(regexp = "^[A-Za-z0-9_-]{3,64}$", message = "must contain 3-64 letters, digits, '-' or '_'")
            String sku,
            @Schema(example = "Mechanical Keyboard") @NotBlank @Size(max = 150) String name,
            @Schema(example = "Teclado mecánico") @Size(max = 2000) String description,
            @Schema(example = "89.90") @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal price,
            @Schema(example = "25") @NotNull @PositiveOrZero @Max(MAX_STOCK_ADJUSTMENT) Integer initialStock
    ) {
    }

    public record UpdateProductRequest(
            @Schema(example = "Mechanical Keyboard") @NotBlank @Size(max = 150) String name,
            @Size(max = 2000) String description,
            @Schema(example = "79.90") @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal price,
            @Schema(example = "true") @NotNull Boolean active
    ) {
    }

    public record ProductResponse(Long id, String sku, String name, String description, BigDecimal price,
                                  boolean active, Instant createdAt, Instant updatedAt) {

        public static ProductResponse from(Product product) {
            return new ProductResponse(product.getId(), product.getSku(), product.getName(), product.getDescription(),
                    product.getPrice(), product.isActive(), product.getCreatedAt(), product.getUpdatedAt());
        }
    }

    public record StockAdjustmentRequest(
            @Schema(example = "10") @NotNull @Positive @Max(MAX_STOCK_ADJUSTMENT) Integer quantity
    ) {
    }

    public record StockResponse(Long productId, int available, int reserved, Instant updatedAt) {
    }
}
