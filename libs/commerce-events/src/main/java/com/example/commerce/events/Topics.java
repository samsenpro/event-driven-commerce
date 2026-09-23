package com.example.commerce.events;

import java.util.List;

/**
 * Nombres de los topics de Kafka, definidos en un único sitio.
 * <p>
 * Son constantes de compilación para poder usarlas en {@code @KafkaListener(topics = ...)}.
 * Convención: {@code <agregado>.<hecho en pasado>}. Cada topic tiene su Dead Letter Topic {@code <topic>.DLT}.
 */
public final class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String ORDERS_CONFIRMED = "orders.confirmed";
    public static final String ORDERS_CANCELLED = "orders.cancelled";

    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_FAILED = "inventory.failed";
    public static final String INVENTORY_RELEASED = "inventory.released";

    public static final String PAYMENTS_REQUESTED = "payments.requested";
    public static final String PAYMENTS_APPROVED = "payments.approved";
    public static final String PAYMENTS_REJECTED = "payments.rejected";

    public static final String SHIPMENTS_CREATED = "shipments.created";

    public static final String PRODUCTS_CHANGED = "products.changed";

    public static final String DLT_SUFFIX = ".DLT";

    public static final List<String> ALL = List.of(
            ORDERS_CREATED, ORDERS_CONFIRMED, ORDERS_CANCELLED,
            INVENTORY_RESERVED, INVENTORY_FAILED, INVENTORY_RELEASED,
            PAYMENTS_REQUESTED, PAYMENTS_APPROVED, PAYMENTS_REJECTED,
            SHIPMENTS_CREATED,
            PRODUCTS_CHANGED
    );

    private Topics() {
    }

    public static String deadLetterOf(String topic) {
        return topic + DLT_SUFFIX;
    }
}
