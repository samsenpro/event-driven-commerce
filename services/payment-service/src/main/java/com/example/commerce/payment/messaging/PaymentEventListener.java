package com.example.commerce.payment.messaging;

import com.example.commerce.events.EventType;
import com.example.commerce.events.Topics;
import com.example.commerce.payment.service.PaymentService;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidores del payment-service (consumer group {@code payment-service-group}).
 */
@Component
public class PaymentEventListener {

    private final IdempotentEventProcessor processor;
    private final PaymentService paymentService;

    public PaymentEventListener(IdempotentEventProcessor processor, PaymentService paymentService) {
        this.processor = processor;
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = Topics.PAYMENTS_REQUESTED)
    public void onPaymentRequested(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.PAYMENT_REQUESTED, "charge", paymentService::process);
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED)
    public void onOrderCancelled(ConsumerRecord<String, String> record) {
        processor.handle(record, EventType.ORDER_CANCELLED, "refund", paymentService::onOrderCancelled);
    }
}
