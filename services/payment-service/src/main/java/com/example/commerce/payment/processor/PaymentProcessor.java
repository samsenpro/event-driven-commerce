package com.example.commerce.payment.processor;

import java.math.BigDecimal;

/**
 * Pasarela de pago. En este proyecto es un simulador; una integración real (Stripe, Adyen...)
 * implementaría esta interfaz usando el orderId como clave de idempotencia del proveedor.
 */
public interface PaymentProcessor {

    /**
     * @return resultado de negocio (aprobado o rechazado)
     * @throws PaymentGatewayUnavailableException fallo transitorio: el evento se reintentará
     */
    PaymentResult charge(Long orderId, Long userId, BigDecimal amount);

    record PaymentResult(boolean approved, String reason) {

        public static PaymentResult approvedPayment() {
            return new PaymentResult(true, null);
        }

        public static PaymentResult rejectedPayment(String reason) {
            return new PaymentResult(false, reason);
        }
    }
}
