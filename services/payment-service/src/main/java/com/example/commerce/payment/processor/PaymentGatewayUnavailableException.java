package com.example.commerce.payment.processor;

/**
 * Error transitorio de la pasarela (timeout, 503...). No es un rechazo del pago: el consumidor lo
 * reintenta con backoff y, si persiste, el evento termina en el DLT para revisión.
 */
public class PaymentGatewayUnavailableException extends RuntimeException {

    public PaymentGatewayUnavailableException(String message) {
        super(message);
    }
}
