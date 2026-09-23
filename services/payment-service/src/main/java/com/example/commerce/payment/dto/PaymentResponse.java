package com.example.commerce.payment.dto;

import com.example.commerce.payment.entity.Payment;
import com.example.commerce.payment.entity.PaymentStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentResponse(UUID id, Long orderId, Long userId, BigDecimal amount, PaymentStatus status,
                              String reason, Instant createdAt, Instant updatedAt) {

    public static PaymentResponse from(Payment payment) {
        return new PaymentResponse(payment.getId(), payment.getOrderId(), payment.getUserId(), payment.getAmount(),
                payment.getStatus(), payment.getReason(), payment.getCreatedAt(), payment.getUpdatedAt());
    }
}
