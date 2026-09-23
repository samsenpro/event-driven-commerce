package com.example.commerce.platform.messaging;

/**
 * Error permanente al procesar un evento: reintentarlo no cambiaría el resultado. El
 * {@code DefaultErrorHandler} no lo reintenta y envía el mensaje directamente al Dead Letter Topic.
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
