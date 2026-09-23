package com.example.commerce.order.entity;

import java.util.EnumSet;
import java.util.Set;

/**
 * Estados del pedido a lo largo de la saga.
 * <pre>
 * PENDING ──inventory.reserved──► INVENTORY_RESERVED ──payments.approved──► CONFIRMED ──shipments.created──► SHIPPED
 *    │                                  │                                     │
 *    ├──inventory.failed───────────────►│◄──payments.rejected                 │
 *    │                                  ▼                                     │
 *    └────────── cancelación del cliente ──────────────► CANCELLED ◄──────────┘
 * </pre>
 */
public enum OrderStatus {
    PENDING,
    INVENTORY_RESERVED,
    CONFIRMED,
    SHIPPED,
    CANCELLED;

    public Set<OrderStatus> allowedTransitions() {
        return switch (this) {
            case PENDING -> EnumSet.of(INVENTORY_RESERVED, CANCELLED);
            case INVENTORY_RESERVED -> EnumSet.of(CONFIRMED, CANCELLED);
            case CONFIRMED -> EnumSet.of(SHIPPED, CANCELLED);
            case SHIPPED, CANCELLED -> EnumSet.noneOf(OrderStatus.class);
        };
    }

    public boolean canTransitionTo(OrderStatus target) {
        return allowedTransitions().contains(target);
    }
}
