package com.example.commerce.shipping.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "shipments")
public class Shipment {

    private static final int TRACKING_SUFFIX_LENGTH = 10;

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private Long orderId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ShipmentStatus status;

    @Column(name = "tracking_number", length = 40)
    private String trackingNumber;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Shipment() {
        // Requerido por JPA
    }

    private Shipment(Long orderId, Long userId, ShipmentStatus status, String trackingNumber, Instant now) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.userId = userId;
        this.status = status;
        this.trackingNumber = trackingNumber;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Shipment create(Long orderId, Long userId, Instant now) {
        String tracking = "TRK-" + UUID.randomUUID().toString().replace("-", "")
                .substring(0, TRACKING_SUFFIX_LENGTH).toUpperCase();
        return new Shipment(orderId, userId, ShipmentStatus.CREATED, tracking, now);
    }

    public static Shipment voided(Long orderId, Long userId, Instant now) {
        return new Shipment(orderId, userId, ShipmentStatus.VOIDED, null, now);
    }

    public void cancel(Instant now) {
        this.status = ShipmentStatus.CANCELLED;
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

    public ShipmentStatus getStatus() {
        return status;
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
