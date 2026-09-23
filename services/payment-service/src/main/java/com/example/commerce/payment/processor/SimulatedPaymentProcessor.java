package com.example.commerce.payment.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.random.RandomGenerator;

/**
 * Simulador de pasarela:
 * <ul>
 *     <li>importe &gt; {@code approval-limit} → PAYMENT_REJECTED (error de negocio, no se reintenta),</li>
 *     <li>con probabilidad {@code transient-failure-rate} → fallo transitorio (se reintenta y puede
 *     acabar en el DLT),</li>
 *     <li>en otro caso → PAYMENT_APPROVED.</li>
 * </ul>
 */
@Component
public class SimulatedPaymentProcessor implements PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(SimulatedPaymentProcessor.class);

    private final PaymentSimulatorProperties properties;
    private final RandomGenerator random;

    @Autowired
    public SimulatedPaymentProcessor(PaymentSimulatorProperties properties) {
        this(properties, RandomGenerator.getDefault());
    }

    SimulatedPaymentProcessor(PaymentSimulatorProperties properties, RandomGenerator random) {
        this.properties = properties;
        this.random = random;
    }

    @Override
    public PaymentResult charge(Long orderId, Long userId, BigDecimal amount) {
        if (random.nextDouble() < properties.transientFailureRate()) {
            log.warn("Simulated payment gateway failure orderId={}", orderId);
            throw new PaymentGatewayUnavailableException("Payment gateway temporarily unavailable");
        }
        if (amount.compareTo(properties.approvalLimit()) > 0) {
            return PaymentResult.rejectedPayment("Amount exceeds approval limit of " + properties.approvalLimit());
        }
        return PaymentResult.approvedPayment();
    }
}
