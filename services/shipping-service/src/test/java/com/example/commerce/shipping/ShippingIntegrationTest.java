package com.example.commerce.shipping;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.Topics;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.shipping.entity.Shipment;
import com.example.commerce.shipping.entity.ShipmentStatus;
import com.example.commerce.shipping.repository.ShipmentRepository;
import com.example.commerce.testing.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ShippingIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private ShipmentRepository repository;

    @Test
    void confirmedOrderCreatesAShipmentAndPublishesIt() throws Exception {
        long orderId = uniqueId();

        events.send(events.envelope(new OrderConfirmed(orderId, 3L, new BigDecimal("40.00")), orderId, "cid-ship"));

        Shipment shipment = awaitShipment(orderId, ShipmentStatus.CREATED);
        JsonNode created = topics.body(topics.awaitRecord(Topics.SHIPMENTS_CREATED, orderId));
        assertThat(created.get("correlationId").asText()).isEqualTo("cid-ship");
        assertThat(created.get("payload").get("trackingNumber").asText()).isEqualTo(shipment.getTrackingNumber());
        mockMvc.perform(get("/api/v1/shipments/{orderId}", orderId).header(HttpHeaders.AUTHORIZATION, adminToken()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CREATED"));
    }

    @Test
    void duplicatedConfirmationCreatesOneShipment() {
        long orderId = uniqueId();
        EventEnvelope<OrderConfirmed> event = events.envelope(new OrderConfirmed(orderId, 3L, BigDecimal.TEN), orderId, "d");

        events.send(event);
        events.send(event);
        awaitShipment(orderId, ShipmentStatus.CREATED);

        assertThat(topics.collect(Topics.SHIPMENTS_CREATED, record -> String.valueOf(orderId).equals(record.key()),
                Duration.ofSeconds(3))).hasSize(1);
    }

    @Test
    void cancellationStopsTheShipmentOrPreventsIt() {
        long shipped = uniqueId();
        events.publish(new OrderConfirmed(shipped, 3L, BigDecimal.TEN), shipped);
        awaitShipment(shipped, ShipmentStatus.CREATED);
        events.publish(new OrderCancelled(shipped, 3L, "CUSTOMER_REQUEST"), shipped);
        awaitShipment(shipped, ShipmentStatus.CANCELLED);

        long notYetConfirmed = uniqueId();
        events.publish(new OrderCancelled(notYetConfirmed, 3L, "CUSTOMER_REQUEST"), notYetConfirmed);
        awaitShipment(notYetConfirmed, ShipmentStatus.VOIDED);
        events.publish(new OrderConfirmed(notYetConfirmed, 3L, BigDecimal.TEN), notYetConfirmed);
        assertThat(topics.collect(Topics.SHIPMENTS_CREATED,
                record -> String.valueOf(notYetConfirmed).equals(record.key()), Duration.ofSeconds(3))).isEmpty();
    }

    private Shipment awaitShipment(long orderId, ShipmentStatus expected) {
        await().atMost(Duration.ofSeconds(20)).until(() -> repository.findByOrderId(orderId)
                .map(Shipment::getStatus).orElse(null) == expected);
        return repository.findByOrderId(orderId).orElseThrow();
    }
}
