package com.example.commerce.order.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Copia local de un producto del catálogo, mantenida a partir del topic {@code products.changed}.
 * El order-service no consulta la base de datos del inventory-service ni lo llama por REST.
 */
@Entity
@Table(name = "product_catalog")
public class CatalogProduct {

    @Id
    @Column(name = "product_id")
    private Long productId;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false, length = 150)
    private String name;

    @Column(nullable = false)
    private BigDecimal price;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected CatalogProduct() {
        // Requerido por JPA
    }

    public CatalogProduct(Long productId, String sku, String name, BigDecimal price, boolean active,
                          Instant updatedAt) {
        this.productId = productId;
        apply(sku, name, price, active, updatedAt);
    }

    public final void apply(String sku, String name, BigDecimal price, boolean active, Instant updatedAt) {
        this.sku = sku;
        this.name = name;
        this.price = price;
        this.active = active;
        this.updatedAt = updatedAt;
    }

    /** Descarta eventos más antiguos que el estado ya aplicado (p. ej. reenvíos tras un reinicio). */
    public boolean isNewerThan(Instant candidate) {
        return !updatedAt.isBefore(candidate);
    }

    public Long getProductId() {
        return productId;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public boolean isActive() {
        return active;
    }
}
