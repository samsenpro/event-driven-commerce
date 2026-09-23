package com.example.commerce.order.messaging;

import com.example.commerce.events.EventType;
import com.example.commerce.events.Topics;
import com.example.commerce.order.service.CatalogReplicaService;
import com.example.commerce.order.service.OrderSagaService;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidores del order-service (consumer group {@code order-service-group}). Cada listener solo
 * traduce el mensaje; la lógica está en los servicios y el procesamiento es idempotente.
 */
@Component
public class OrderEventListener {

    private final IdempotentEventProcessor processor;
    private final OrderSagaService saga;
    private final CatalogReplicaService catalog;

    public OrderEventListener(IdempotentEventProcessor processor, OrderSagaService saga, CatalogReplicaService catalog) {
        this.processor = processor;
        this.saga = saga;
        this.catalog = catalog;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED)
    public void onInventoryReserved(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.INVENTORY_RESERVED, "inventory-reserved", saga::onInventoryReserved);
    }

    @KafkaListener(topics = Topics.INVENTORY_FAILED)
    public void onInventoryReservationFailed(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.INVENTORY_RESERVATION_FAILED, "inventory-failed",
                saga::onInventoryReservationFailed);
    }

    @KafkaListener(topics = Topics.PAYMENTS_APPROVED)
    public void onPaymentApproved(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.PAYMENT_APPROVED, "payment-approved", saga::onPaymentApproved);
    }

    @KafkaListener(topics = Topics.PAYMENTS_REJECTED)
    public void onPaymentRejected(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.PAYMENT_REJECTED, "payment-rejected", saga::onPaymentRejected);
    }

    @KafkaListener(topics = Topics.SHIPMENTS_CREATED)
    public void onShipmentCreated(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.SHIPMENT_CREATED, "shipment-created", saga::onShipmentCreated);
    }

    @KafkaListener(topics = Topics.PRODUCTS_CHANGED)
    public void onProductChanged(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.PRODUCT_CHANGED, "catalog-replica", catalog::apply);
    }
}
