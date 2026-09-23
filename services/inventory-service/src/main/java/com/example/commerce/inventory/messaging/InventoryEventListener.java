package com.example.commerce.inventory.messaging;

import com.example.commerce.events.EventType;
import com.example.commerce.events.Topics;
import com.example.commerce.inventory.service.ReservationService;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidores del inventory-service (consumer group {@code inventory-service-group}).
 * {@code orders.created} lo reciben también, de forma independiente, notification-service-group
 * y cualquier otro grupo suscrito: cada grupo tiene sus propios offsets.
 */
@Component
public class InventoryEventListener {

    private final IdempotentEventProcessor processor;
    private final ReservationService reservations;

    public InventoryEventListener(IdempotentEventProcessor processor, ReservationService reservations) {
        this.processor = processor;
        this.reservations = reservations;
    }

    @KafkaListener(topics = Topics.ORDERS_CREATED)
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CREATED, "reserve-stock", reservations::reserve);
    }

    @KafkaListener(topics = Topics.ORDERS_CONFIRMED)
    public void onOrderConfirmed(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CONFIRMED, "commit-stock", reservations::commit);
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED)
    public void onOrderCancelled(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CANCELLED, "release-stock", reservations::release);
    }
}
