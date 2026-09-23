package com.example.commerce.inventory.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Stock de un producto. Solo se lee con JPA; las escrituras son UPDATE atómicos en
 * {@code StockItemRepository} para que ninguna carrera pueda dejar stock negativo.
 * <ul>
 *     <li>{@code available}: unidades que se pueden reservar.</li>
 *     <li>{@code reserved}: unidades comprometidas en pedidos aún no confirmados.</li>
 * </ul>
 */
@Entity
@Table(name = "stock_items")
public class StockItem {

    @Id
    @Column(name = "product_id")
    private Long productId;

    @Column(nullable = false)
    private int available;

    @Column(nullable = false)
    private int reserved;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected StockItem() {
        // Requerido por JPA
    }

    public StockItem(Long productId, int available, Instant now) {
        this.productId = productId;
        this.available = available;
        this.reserved = 0;
        this.updatedAt = now;
    }

    public Long getProductId() {
        return productId;
    }

    public int getAvailable() {
        return available;
    }

    public int getReserved() {
        return reserved;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
