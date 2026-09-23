package com.example.commerce.notification.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.events.payload.PaymentApproved;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.ShipmentCreated;
import com.example.commerce.notification.dto.NotificationResponse;
import com.example.commerce.notification.entity.Notification;
import com.example.commerce.notification.entity.NotificationType;
import com.example.commerce.notification.gateway.EmailGateway;
import com.example.commerce.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.List;

/**
 * Traduce eventos de la saga en notificaciones al cliente. Cada handler se ejecuta en la
 * transacción del {@code IdempotentEventProcessor}: un evento repetido no genera un segundo email.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repository;
    private final EmailGateway emailGateway;
    private final Clock clock;

    public NotificationService(NotificationRepository repository, EmailGateway emailGateway, Clock clock) {
        this.repository = repository;
        this.emailGateway = emailGateway;
        this.clock = clock;
    }

    public void onOrderCreated(EventEnvelope<OrderCreated> event) {
        OrderCreated order = event.payload();
        notify(event, order.orderId(), order.userId(), NotificationType.ORDER_CREATED,
                "We received your order #" + order.orderId() + " for " + order.totalAmount() + ".");
    }

    public void onPaymentApproved(EventEnvelope<PaymentApproved> event) {
        PaymentApproved payment = event.payload();
        notify(event, payment.orderId(), payment.userId(), NotificationType.PAYMENT_APPROVED,
                "Payment of " + payment.amount() + " for order #" + payment.orderId() + " was approved.");
    }

    public void onPaymentRejected(EventEnvelope<PaymentRejected> event) {
        PaymentRejected payment = event.payload();
        notify(event, payment.orderId(), payment.userId(), NotificationType.PAYMENT_REJECTED,
                "Payment for order #" + payment.orderId() + " was rejected: " + payment.reason());
    }

    public void onOrderCancelled(EventEnvelope<OrderCancelled> event) {
        OrderCancelled order = event.payload();
        notify(event, order.orderId(), order.userId(), NotificationType.ORDER_CANCELLED,
                "Order #" + order.orderId() + " was cancelled (" + order.reason() + ").");
    }

    public void onShipmentCreated(EventEnvelope<ShipmentCreated> event) {
        ShipmentCreated shipment = event.payload();
        notify(event, shipment.orderId(), shipment.userId(), NotificationType.ORDER_SHIPPED,
                "Order #" + shipment.orderId() + " has shipped. Tracking number: " + shipment.trackingNumber());
    }

    @Transactional(readOnly = true)
    public List<NotificationResponse> findByOrderId(Long orderId) {
        return repository.findAllByOrderIdOrderBySentAtAsc(orderId).stream().map(NotificationResponse::from).toList();
    }

    private void notify(EventEnvelope<?> event, Long orderId, Long userId, NotificationType type, String message) {
        String recipient = recipientOf(userId);
        emailGateway.send(recipient, subjectOf(type, orderId), message);
        repository.save(new Notification(event.eventId(), orderId, userId, type, recipient, message, clock.instant()));
        log.info("Notification sent type={} orderId={} userId={}", type, orderId, userId);
    }

    /** No hay servicio de usuarios aquí: el email real se resolvería con un perfil replicado por eventos. */
    private static String recipientOf(Long userId) {
        return "user-" + userId + "@customers.example";
    }

    private static String subjectOf(NotificationType type, Long orderId) {
        return type.name().replace('_', ' ') + " - order #" + orderId;
    }
}
