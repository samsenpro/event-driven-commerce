package com.example.commerce.shipping.entity;

public enum ShipmentStatus {
    CREATED,
    /** El pedido se canceló después de crear el envío: se detiene. */
    CANCELLED,
    /** La cancelación llegó antes que la confirmación: marcador para no crear el envío después. */
    VOIDED
}
