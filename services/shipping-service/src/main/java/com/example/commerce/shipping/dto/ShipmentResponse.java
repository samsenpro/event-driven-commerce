package com.example.commerce.shipping.dto;

import com.example.commerce.shipping.entity.Shipment;
import com.example.commerce.shipping.entity.ShipmentStatus;

import java.time.Instant;
import java.util.UUID;

public record ShipmentResponse(UUID id, Long orderId, Long userId, ShipmentStatus status, String trackingNumber,
                               Instant createdAt, Instant updatedAt) {

    public static ShipmentResponse from(Shipment shipment) {
        return new ShipmentResponse(shipment.getId(), shipment.getOrderId(), shipment.getUserId(), shipment.getStatus(),
                shipment.getTrackingNumber(), shipment.getCreatedAt(), shipment.getUpdatedAt());
    }
}
