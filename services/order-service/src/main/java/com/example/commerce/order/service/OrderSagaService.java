package com.example.commerce.order.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.InventoryReservationFailed;
import com.example.commerce.events.payload.InventoryReserved;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.events.payload.PaymentApproved;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.PaymentRequested;
import com.example.commerce.events.payload.ShipmentCreated;
import com.example.commerce.order.entity.CancellationReason;
import com.example.commerce.order.entity.Order;
import com.example.commerce.order.entity.OrderStatus;
import com.example.commerce.order.repository.OrderRepository;
import com.example.commerce.platform.messaging.NonRetryableEventException;
import com.example.commerce.platform.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;

/**
 * Participación del order-service en la saga (coreografía): reacciona a los hechos que publican
 * los demás servicios y publica los suyos. Cada método se ejecuta dentro de la transacción del
 * {@code IdempotentEventProcessor}: cambio de estado + evento de salida en el outbox + registro del
 * evento procesado son atómicos.
 * <p>
 * Un evento que llega cuando el pedido ya no está en el estado esperado (p. ej. el pago se aprueba
 * después de que el cliente cancelara) se ignora: la compensación la hace quien corresponde al
 * recibir ORDER_CANCELLED (payment reembolsa, inventory libera).
 */
@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

    private final OrderRepository orderRepository;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderSagaService(OrderRepository orderRepository, OutboxWriter outbox, Clock clock) {
        this.orderRepository = orderRepository;
        this.outbox = outbox;
        this.clock = clock;
    }

    /** Stock reservado → se solicita el cobro. */
    public void onInventoryReserved(EventEnvelope<InventoryReserved> event) {
        Order order = load(event.payload().orderId());
        if (!transitionAllowed(order, OrderStatus.INVENTORY_RESERVED, event)) {
            return;
        }
        order.markInventoryReserved(clock.instant());
        outbox.publish(new PaymentRequested(order.getId(), order.getUserId(), order.getTotalAmount()), order.getId());
        log.info("Payment requested orderId={} amount={}", order.getId(), order.getTotalAmount());
    }

    /** Sin stock → el pedido se cancela (no hay nada que compensar). */
    public void onInventoryReservationFailed(EventEnvelope<InventoryReservationFailed> event) {
        cancel(event, event.payload().orderId(), OrderStatus.PENDING, CancellationReason.OUT_OF_STOCK);
    }

    /** Pago aprobado → pedido confirmado. */
    public void onPaymentApproved(EventEnvelope<PaymentApproved> event) {
        Order order = load(event.payload().orderId());
        if (order.getStatus() != OrderStatus.INVENTORY_RESERVED) {
            ignore(order, event);
            return;
        }
        order.confirm(clock.instant());
        outbox.publish(new OrderConfirmed(order.getId(), order.getUserId(), order.getTotalAmount()), order.getId());
        log.info("Order confirmed orderId={} userId={}", order.getId(), order.getUserId());
    }

    /** Pago rechazado → compensación: se cancela el pedido e inventory libera el stock reservado. */
    public void onPaymentRejected(EventEnvelope<PaymentRejected> event) {
        cancel(event, event.payload().orderId(), OrderStatus.INVENTORY_RESERVED, CancellationReason.PAYMENT_REJECTED);
    }

    public void onShipmentCreated(EventEnvelope<ShipmentCreated> event) {
        Order order = load(event.payload().orderId());
        if (!transitionAllowed(order, OrderStatus.SHIPPED, event)) {
            return;
        }
        order.markShipped(clock.instant());
        log.info("Order shipped orderId={} tracking={}", order.getId(), event.payload().trackingNumber());
    }

    private void cancel(EventEnvelope<?> event, Long orderId, OrderStatus expected, CancellationReason reason) {
        Order order = load(orderId);
        if (order.getStatus() != expected) {
            ignore(order, event);
            return;
        }
        order.cancel(reason, clock.instant());
        outbox.publish(new OrderCancelled(order.getId(), order.getUserId(), reason.name()), order.getId());
        log.info("Order cancelled orderId={} reason={}", order.getId(), reason);
    }

    private boolean transitionAllowed(Order order, OrderStatus target, EventEnvelope<?> event) {
        if (order.canMoveTo(target)) {
            return true;
        }
        ignore(order, event);
        return false;
    }

    private static void ignore(Order order, EventEnvelope<?> event) {
        log.info("Event ignored: order {} is {} (event={})", order.getId(), order.getStatus(), event.eventType());
    }

    /**
     * Los eventos de la saga solo existen para pedidos creados por este servicio. Si el pedido no
     * existe, el evento es incoherente y reintentarlo no lo arreglará: va al DLT.
     */
    private Order load(Long orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NonRetryableEventException("Unknown order " + orderId));
    }
}
