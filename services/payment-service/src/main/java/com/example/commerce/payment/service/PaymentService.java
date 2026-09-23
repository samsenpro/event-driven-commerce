package com.example.commerce.payment.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.PaymentApproved;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.PaymentRequested;
import com.example.commerce.payment.dto.PaymentResponse;
import com.example.commerce.payment.entity.Payment;
import com.example.commerce.payment.entity.PaymentStatus;
import com.example.commerce.payment.processor.PaymentProcessor;
import com.example.commerce.payment.processor.PaymentProcessor.PaymentResult;
import com.example.commerce.payment.repository.PaymentRepository;
import com.example.commerce.platform.outbox.OutboxWriter;
import com.example.commerce.platform.web.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;

/**
 * Participación del payment-service en la saga. Los handlers de eventos se ejecutan en la
 * transacción del {@code IdempotentEventProcessor}.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final PaymentProcessor paymentProcessor;
    private final OutboxWriter outbox;
    private final Clock clock;

    public PaymentService(PaymentRepository paymentRepository, PaymentProcessor paymentProcessor,
                          OutboxWriter outbox, Clock clock) {
        this.paymentRepository = paymentRepository;
        this.paymentProcessor = paymentProcessor;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Cobra el pedido una sola vez. Un fallo transitorio de la pasarela lanza una excepción: la
     * transacción hace rollback (también el registro del evento) y Kafka lo reintenta.
     */
    public void process(EventEnvelope<PaymentRequested> event) {
        PaymentRequested request = event.payload();
        Optional<Payment> existing = paymentRepository.findByOrderId(request.orderId());
        if (existing.isPresent()) {
            log.info("Payment for orderId={} already {}; PAYMENT_REQUESTED ignored",
                    request.orderId(), existing.get().getStatus());
            return;
        }

        PaymentResult result = paymentProcessor.charge(request.orderId(), request.userId(), request.amount());
        if (result.approved()) {
            Payment payment = paymentRepository.save(
                    Payment.approved(request.orderId(), request.userId(), request.amount(), clock.instant()));
            outbox.publish(new PaymentApproved(request.orderId(), request.userId(), payment.getId(), request.amount()),
                    request.orderId());
            log.info("Payment approved orderId={} amount={}", request.orderId(), request.amount());
        } else {
            Payment payment = paymentRepository.save(Payment.rejected(request.orderId(), request.userId(),
                    request.amount(), result.reason(), clock.instant()));
            outbox.publish(new PaymentRejected(request.orderId(), request.userId(), payment.getId(), result.reason()),
                    request.orderId());
            log.info("Payment rejected orderId={} reason={}", request.orderId(), result.reason());
        }
    }

    /**
     * Compensación: si el pedido se cancela después de cobrar, se reembolsa. Si la cancelación
     * llega antes que la solicitud de cobro, se deja un marcador para no cobrar después.
     */
    public void onOrderCancelled(EventEnvelope<OrderCancelled> event) {
        OrderCancelled cancelled = event.payload();
        paymentRepository.findByOrderId(cancelled.orderId()).ifPresentOrElse(
                payment -> {
                    if (payment.getStatus() == PaymentStatus.APPROVED) {
                        payment.refund(clock.instant());
                        log.info("Payment refunded orderId={} amount={}", cancelled.orderId(), payment.getAmount());
                    }
                },
                () -> {
                    paymentRepository.save(Payment.voided(cancelled.orderId(), cancelled.userId(), clock.instant()));
                    log.info("ORDER_CANCELLED arrived before PAYMENT_REQUESTED; payment voided orderId={}",
                            cancelled.orderId());
                });
    }

    @Transactional(readOnly = true)
    public PaymentResponse findByOrderId(Long orderId) {
        return paymentRepository.findByOrderId(orderId)
                .map(PaymentResponse::from)
                .orElseThrow(() -> new NotFoundException("No payment for order " + orderId));
    }
}
