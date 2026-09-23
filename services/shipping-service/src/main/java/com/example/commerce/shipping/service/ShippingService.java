package com.example.commerce.shipping.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.events.payload.ShipmentCreated;
import com.example.commerce.platform.outbox.OutboxWriter;
import com.example.commerce.platform.web.NotFoundException;
import com.example.commerce.shipping.dto.ShipmentResponse;
import com.example.commerce.shipping.entity.Shipment;
import com.example.commerce.shipping.entity.ShipmentStatus;
import com.example.commerce.shipping.repository.ShipmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;

/**
 * Crea el envío cuando el pedido está confirmado. ORDER_CONFIRMED ya implica que el stock está
 * reservado y el pago aprobado, así que este servicio no necesita correlacionar ambos eventos.
 */
@Service
public class ShippingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);

    private final ShipmentRepository repository;
    private final OutboxWriter outbox;
    private final Clock clock;

    public ShippingService(ShipmentRepository repository, OutboxWriter outbox, Clock clock) {
        this.repository = repository;
        this.outbox = outbox;
        this.clock = clock;
    }

    public void onOrderConfirmed(EventEnvelope<OrderConfirmed> event) {
        OrderConfirmed order = event.payload();
        Optional<Shipment> existing = repository.findByOrderId(order.orderId());
        if (existing.isPresent()) {
            log.info("Shipment for orderId={} already {}; ORDER_CONFIRMED ignored",
                    order.orderId(), existing.get().getStatus());
            return;
        }
        Shipment shipment = repository.save(Shipment.create(order.orderId(), order.userId(), clock.instant()));
        outbox.publish(new ShipmentCreated(order.orderId(), order.userId(), shipment.getId(),
                shipment.getTrackingNumber()), order.orderId());
        log.info("Shipment created orderId={} tracking={}", order.orderId(), shipment.getTrackingNumber());
    }

    public void onOrderCancelled(EventEnvelope<OrderCancelled> event) {
        OrderCancelled order = event.payload();
        repository.findByOrderId(order.orderId()).ifPresentOrElse(
                shipment -> {
                    if (shipment.getStatus() == ShipmentStatus.CREATED) {
                        shipment.cancel(clock.instant());
                        log.info("Shipment cancelled orderId={}", order.orderId());
                    }
                },
                () -> repository.save(Shipment.voided(order.orderId(), order.userId(), clock.instant())));
    }

    @Transactional(readOnly = true)
    public ShipmentResponse findByOrderId(Long orderId) {
        return repository.findByOrderId(orderId)
                .map(ShipmentResponse::from)
                .orElseThrow(() -> new NotFoundException("No shipment for order " + orderId));
    }
}
