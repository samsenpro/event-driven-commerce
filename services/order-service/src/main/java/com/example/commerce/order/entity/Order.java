package com.example.commerce.order.entity;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Pedido. Las transiciones de estado están encapsuladas y validadas por {@link OrderStatus}.
 * {@code @Version} detecta modificaciones concurrentes (p. ej. el cliente cancela mientras llega
 * la confirmación del pago): la segunda transacción falla y se reintenta con el estado actualizado.
 */
@Entity
@Table(name = "orders")
public class Order {

    private static final int MONEY_SCALE = 2;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private OrderStatus status;

    @Column(name = "total_amount", nullable = false)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "cancellation_reason", length = 50)
    private CancellationReason cancellationReason;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "order_items", joinColumns = @JoinColumn(name = "order_id"))
    @OrderBy("productId")
    private List<OrderItem> items = new ArrayList<>();

    @Version
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {
        // Requerido por JPA
    }

    public static Order place(Long userId, List<OrderItem> items, Instant now) {
        Order order = new Order();
        order.userId = userId;
        order.items = new ArrayList<>(items);
        order.totalAmount = items.stream()
                .map(OrderItem::subtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(MONEY_SCALE, RoundingMode.UNNECESSARY);
        order.status = OrderStatus.PENDING;
        order.createdAt = now;
        order.updatedAt = now;
        return order;
    }

    public boolean canMoveTo(OrderStatus target) {
        return status.canTransitionTo(target);
    }

    public void markInventoryReserved(Instant now) {
        moveTo(OrderStatus.INVENTORY_RESERVED, now);
    }

    public void confirm(Instant now) {
        moveTo(OrderStatus.CONFIRMED, now);
    }

    public void markShipped(Instant now) {
        moveTo(OrderStatus.SHIPPED, now);
    }

    public void cancel(CancellationReason reason, Instant now) {
        moveTo(OrderStatus.CANCELLED, now);
        this.cancellationReason = reason;
    }

    public boolean isOwnedBy(Long candidateUserId) {
        return userId.equals(candidateUserId);
    }

    private void moveTo(OrderStatus target, Instant now) {
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException("Order " + id + " cannot move from " + status + " to " + target);
        }
        this.status = target;
        this.updatedAt = now;
    }

    public Long getId() {
        return id;
    }

    public Long getUserId() {
        return userId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public CancellationReason getCancellationReason() {
        return cancellationReason;
    }

    public List<OrderItem> getItems() {
        return List.copyOf(items);
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
