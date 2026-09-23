package com.example.commerce.events;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.UUID;

/**
 * Estructura común de todos los eventos del sistema.
 *
 * @param eventId       identificador único del evento; es la clave de idempotencia de los consumidores
 * @param eventType     tipo de evento (p. ej. {@code ORDER_CREATED}), ver {@link EventType}
 * @param eventVersion  versión del esquema del payload; los consumidores rechazan versiones que no conocen
 * @param occurredAt    instante en que ocurrió el hecho de negocio
 * @param aggregateType tipo de agregado que originó el evento (Order, Product...)
 * @param aggregateId   id del agregado; se usa como clave de Kafka para mantener el orden por agregado
 * @param correlationId id que se conserva a lo largo de toda la saga para seguir un pedido entre servicios
 * @param source        servicio que publicó el evento
 * @param payload       datos del hecho de negocio
 */
public record EventEnvelope<T>(
        @NotNull UUID eventId,
        @NotBlank String eventType,
        @Positive int eventVersion,
        @NotNull Instant occurredAt,
        @NotBlank String aggregateType,
        @NotBlank String aggregateId,
        @NotBlank String correlationId,
        @NotBlank String source,
        @NotNull @Valid T payload
) {
}
