package com.example.commerce.notification.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Registro de una notificación enviada (histórico y auditoría).
 */
@Entity
@Table(name = "notifications")
public class Notification {

    public static final String EMAIL_CHANNEL = "EMAIL";

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private NotificationType type;

    @Column(nullable = false, length = 20)
    private String channel;

    @Column(nullable = false)
    private String recipient;

    @Column(nullable = false, length = 500)
    private String message;

    @Column(name = "sent_at", nullable = false)
    private Instant sentAt;

    protected Notification() {
        // Requerido por JPA
    }

    public Notification(UUID eventId, Long orderId, Long userId, NotificationType type, String recipient,
                        String message, Instant sentAt) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.orderId = orderId;
        this.userId = userId;
        this.type = type;
        this.channel = EMAIL_CHANNEL;
        this.recipient = recipient;
        this.message = message;
        this.sentAt = sentAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getUserId() {
        return userId;
    }

    public NotificationType getType() {
        return type;
    }

    public String getChannel() {
        return channel;
    }

    public String getRecipient() {
        return recipient;
    }

    public String getMessage() {
        return message;
    }

    public Instant getSentAt() {
        return sentAt;
    }
}
