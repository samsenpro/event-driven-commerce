package com.example.commerce.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private Long orderId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(length = 200)
    private String reason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Payment() {
        // Requerido por JPA
    }

    private Payment(Long orderId, Long userId, BigDecimal amount, PaymentStatus status, String reason, Instant now) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.status = status;
        this.reason = reason;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Payment approved(Long orderId, Long userId, BigDecimal amount, Instant now) {
        return new Payment(orderId, userId, amount, PaymentStatus.APPROVED, null, now);
    }

    public static Payment rejected(Long orderId, Long userId, BigDecimal amount, String reason, Instant now) {
        return new Payment(orderId, userId, amount, PaymentStatus.REJECTED, reason, now);
    }

    public static Payment voided(Long orderId, Long userId, Instant now) {
        return new Payment(orderId, userId, null, PaymentStatus.VOIDED, "Order cancelled before payment", now);
    }

    public void refund(Instant now) {
        this.status = PaymentStatus.REFUNDED;
        this.reason = "Order cancelled after payment";
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getUserId() {
        return userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
