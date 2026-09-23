package com.example.commerce.platform.messaging;

/**
 * El mensaje no cumple el contrato: JSON ilegible, tipo inesperado, versión desconocida o
 * payload inválido. Va al DLT sin reintentos.
 */
public class InvalidEventException extends NonRetryableEventException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
