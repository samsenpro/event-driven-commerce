package com.example.commerce.notification.messaging;

import com.example.commerce.events.EventType;
import com.example.commerce.events.Topics;
import com.example.commerce.notification.service.NotificationService;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidores del notification-service (consumer group {@code notification-service-group}).
 * Recibe {@code orders.created} y {@code payments.*} de forma independiente a inventory, order
 * y payment: cada consumer group lleva sus propios offsets sobre el mismo topic.
 */
@Component
public class NotificationEventListener {

    private final IdempotentEventProcessor processor;
    private final NotificationService notifications;

    public NotificationEventListener(IdempotentEventProcessor processor, NotificationService notifications) {
        this.processor = processor;
        this.notifications = notifications;
    }

    @KafkaListener(topics = Topics.ORDERS_CREATED)
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CREATED, "order-created", notifications::onOrderCreated);
    }

    @KafkaListener(topics = Topics.PAYMENTS_APPROVED)
    public void onPaymentApproved(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.PAYMENT_APPROVED, "payment-approved", notifications::onPaymentApproved);
    }

    @KafkaListener(topics = Topics.PAYMENTS_REJECTED)
    public void onPaymentRejected(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.PAYMENT_REJECTED, "payment-rejected", notifications::onPaymentRejected);
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED)
    public void onOrderCancelled(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CANCELLED, "order-cancelled", notifications::onOrderCancelled);
    }

    @KafkaListener(topics = Topics.SHIPMENTS_CREATED)
    public void onShipmentCreated(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.SHIPMENT_CREATED, "order-shipped", notifications::onShipmentCreated);
    }
}
