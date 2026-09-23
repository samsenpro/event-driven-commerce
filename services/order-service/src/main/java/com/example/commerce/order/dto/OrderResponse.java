package com.example.commerce.order.dto;

import com.example.commerce.order.entity.CancellationReason;
import com.example.commerce.order.entity.Order;
import com.example.commerce.order.entity.OrderItem;
import com.example.commerce.order.entity.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
        Long id,
        Long userId,
        OrderStatus status,
        BigDecimal totalAmount,
        CancellationReason cancellationReason,
        List<Item> items,
        Instant createdAt,
        Instant updatedAt
) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getUserId(),
                order.getStatus(),
                order.getTotalAmount(),
                order.getCancellationReason(),
                order.getItems().stream().map(Item::from).toList(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }

    public record Item(Long productId, String productName, int quantity, BigDecimal unitPrice, BigDecimal subtotal) {

        static Item from(OrderItem item) {
            return new Item(item.getProductId(), item.getProductName(), item.getQuantity(), item.getUnitPrice(),
                    item.subtotal());
        }
    }
}
