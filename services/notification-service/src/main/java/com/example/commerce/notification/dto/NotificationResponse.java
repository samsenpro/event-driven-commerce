package com.example.commerce.notification.dto;

import com.example.commerce.notification.entity.Notification;
import com.example.commerce.notification.entity.NotificationType;

import java.time.Instant;
import java.util.UUID;

public record NotificationResponse(UUID id, UUID eventId, Long orderId, Long userId, NotificationType type,
                                   String channel, String recipient, String message, Instant sentAt) {

    public static NotificationResponse from(Notification notification) {
        return new NotificationResponse(notification.getId(), notification.getEventId(), notification.getOrderId(),
                notification.getUserId(), notification.getType(), notification.getChannel(),
                notification.getRecipient(), notification.getMessage(), notification.getSentAt());
    }
}
