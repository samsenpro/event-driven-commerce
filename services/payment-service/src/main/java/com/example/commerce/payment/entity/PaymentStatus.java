package com.example.commerce.payment.entity;

public enum PaymentStatus {
    APPROVED,
    REJECTED,
    /** Pedido cancelado después de cobrar: se devuelve el importe. */
    REFUNDED,
    /**
     * La cancelación llegó antes que la solicitud de cobro (topics distintos no garantizan orden).
     * Marcador para que un PAYMENT_REQUESTED tardío no cobre un pedido ya cancelado.
     */
    VOIDED
}
