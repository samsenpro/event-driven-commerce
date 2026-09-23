package com.example.commerce.inventory.entity;

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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reserva de stock de un pedido. Es el estado local que permite compensar: al cancelarse el
 * pedido se sabe exactamente qué unidades devolver.
 */
@Entity
@Table(name = "reservations")
public class Reservation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true)
    private Long orderId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ReservationStatus status;

    @Column(length = 200)
    private String reason;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "reservation_lines", joinColumns = @JoinColumn(name = "reservation_id"))
    @OrderBy("productId")
    private List<ReservationLine> lines = new ArrayList<>();

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Reservation() {
        // Requerido por JPA
    }

    private Reservation(Long orderId, Long userId, ReservationStatus status, String reason,
                        List<ReservationLine> lines, Instant now) {
        this.orderId = orderId;
        this.userId = userId;
        this.status = status;
        this.reason = reason;
        this.lines = new ArrayList<>(lines);
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Reservation reserved(Long orderId, Long userId, List<ReservationLine> lines, Instant now) {
        return new Reservation(orderId, userId, ReservationStatus.RESERVED, null, lines, now);
    }

    public static Reservation rejected(Long orderId, Long userId, String reason, Instant now) {
        return new Reservation(orderId, userId, ReservationStatus.REJECTED, reason, List.of(), now);
    }

    public static Reservation voided(Long orderId, Long userId, Instant now) {
        return new Reservation(orderId, userId, ReservationStatus.VOIDED, "Order cancelled before reservation",
                List.of(), now);
    }

    public void commit(Instant now) {
        this.status = ReservationStatus.COMMITTED;
        this.updatedAt = now;
    }

    public void release(Instant now) {
        this.status = ReservationStatus.RELEASED;
        this.updatedAt = now;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getUserId() {
        return userId;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public List<ReservationLine> getLines() {
        return List.copyOf(lines);
    }
}
