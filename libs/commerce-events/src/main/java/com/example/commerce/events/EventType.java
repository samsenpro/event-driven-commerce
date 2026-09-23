package com.example.commerce.events;

import com.example.commerce.events.payload.InventoryReleased;
import com.example.commerce.events.payload.InventoryReservationFailed;
import com.example.commerce.events.payload.InventoryReserved;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.events.payload.PaymentApproved;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.PaymentRequested;
import com.example.commerce.events.payload.ProductChanged;
import com.example.commerce.events.payload.ShipmentCreated;

/**
 * Catálogo de eventos: para cada tipo, su topic, el agregado que lo origina, la clase del
 * payload y la versión actual del esquema. Productores y consumidores usan esta única fuente.
 * <p>
 * Evolución del esquema: cambios compatibles (añadir campos opcionales) mantienen la versión;
 * un cambio incompatible sube la versión y los consumidores antiguos envían el evento al DLT
 * en lugar de interpretarlo mal.
 */
public enum EventType {

    ORDER_CREATED(Topics.ORDERS_CREATED, AggregateTypes.ORDER, OrderCreated.class, 1),
    ORDER_CONFIRMED(Topics.ORDERS_CONFIRMED, AggregateTypes.ORDER, OrderConfirmed.class, 1),
    ORDER_CANCELLED(Topics.ORDERS_CANCELLED, AggregateTypes.ORDER, OrderCancelled.class, 1),

    INVENTORY_RESERVED(Topics.INVENTORY_RESERVED, AggregateTypes.ORDER, InventoryReserved.class, 1),
    INVENTORY_RESERVATION_FAILED(Topics.INVENTORY_FAILED, AggregateTypes.ORDER, InventoryReservationFailed.class, 1),
    INVENTORY_RELEASED(Topics.INVENTORY_RELEASED, AggregateTypes.ORDER, InventoryReleased.class, 1),

    PAYMENT_REQUESTED(Topics.PAYMENTS_REQUESTED, AggregateTypes.ORDER, PaymentRequested.class, 1),
    PAYMENT_APPROVED(Topics.PAYMENTS_APPROVED, AggregateTypes.ORDER, PaymentApproved.class, 1),
    PAYMENT_REJECTED(Topics.PAYMENTS_REJECTED, AggregateTypes.ORDER, PaymentRejected.class, 1),

    SHIPMENT_CREATED(Topics.SHIPMENTS_CREATED, AggregateTypes.ORDER, ShipmentCreated.class, 1),

    PRODUCT_CHANGED(Topics.PRODUCTS_CHANGED, AggregateTypes.PRODUCT, ProductChanged.class, 1);

    private final String topic;
    private final String aggregateType;
    private final Class<?> payloadType;
    private final int version;

    EventType(String topic, String aggregateType, Class<?> payloadType, int version) {
        this.topic = topic;
        this.aggregateType = aggregateType;
        this.payloadType = payloadType;
        this.version = version;
    }

    public String topic() {
        return topic;
    }

    public String aggregateType() {
        return aggregateType;
    }

    public Class<?> payloadType() {
        return payloadType;
    }

    public int version() {
        return version;
    }

    /** Busca el tipo que corresponde a un payload; cada clase de payload pertenece a un único tipo. */
    public static EventType forPayload(Object payload) {
        for (EventType type : values()) {
            if (type.payloadType.isInstance(payload)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No event type registered for " + payload.getClass().getName());
    }
}
