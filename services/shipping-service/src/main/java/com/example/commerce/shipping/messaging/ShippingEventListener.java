package com.example.commerce.shipping.messaging;

import com.example.commerce.events.EventType;
import com.example.commerce.events.Topics;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import com.example.commerce.shipping.service.ShippingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidores del shipping-service (consumer group {@code shipping-service-group}).
 */
@Component
public class ShippingEventListener {

    private final IdempotentEventProcessor processor;
    private final ShippingService shippingService;

    public ShippingEventListener(IdempotentEventProcessor processor, ShippingService shippingService) {
        this.processor = processor;
        this.shippingService = shippingService;
    }

    @KafkaListener(topics = Topics.ORDERS_CONFIRMED)
    public void onOrderConfirmed(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CONFIRMED, "create-shipment", shippingService::onOrderConfirmed);
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED)
    public void onOrderCancelled(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CANCELLED, "cancel-shipment", shippingService::onOrderCancelled);
    }
}
