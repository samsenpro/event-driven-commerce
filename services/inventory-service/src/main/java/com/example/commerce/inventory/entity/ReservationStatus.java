package com.example.commerce.inventory.entity;

public enum ReservationStatus {
    /** Stock apartado para el pedido. */
    RESERVED,
    /** Pedido confirmado: el stock reservado se ha descontado definitivamente. */
    COMMITTED,
    /** Compensación: el stock volvió a estar disponible (pago rechazado o pedido cancelado). */
    RELEASED,
    /** No había stock suficiente (o algún producto no existe): no se reservó nada. */
    REJECTED,
    /**
     * La cancelación llegó antes que la creación del pedido (topics distintos no garantizan orden).
     * Queda como marcador para que el ORDER_CREATED tardío no reserve stock.
     */
    VOIDED
}
