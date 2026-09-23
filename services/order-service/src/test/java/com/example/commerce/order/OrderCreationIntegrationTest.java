package com.example.commerce.order;

import com.example.commerce.events.Topics;
import com.example.commerce.order.entity.OrderStatus;
import com.example.commerce.platform.outbox.OutboxEvent;
import com.example.commerce.platform.outbox.OutboxStatus;
import com.example.commerce.platform.outbox.OutboxStore;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class OrderCreationIntegrationTest extends OrderServiceIntegrationTest {

    @Autowired
    private OutboxStore outboxStore;

    @Test
    void createdOrderIsStoredWithItsOutboxEventAndPublishedToKafka() throws Exception {
        long keyboard = catalogProduct("89.90", true);
        long mouse = catalogProduct("19.99", true);
        long userId = uniqueId();

        long orderId = placeOrder(userId, "cid-create-1", keyboard, 2, mouse, 1);

        assertThat(orderRepository.findById(orderId)).get()
                .satisfies(order -> {
                    assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
                    assertThat(order.getTotalAmount()).isEqualByComparingTo("199.79");
                });

        // El evento se escribió en el outbox en la misma transacción y el publicador lo envió
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(
                outboxStore.findByAggregate("Order", String.valueOf(orderId)))
                .singleElement()
                .extracting(OutboxEvent::status).isEqualTo(OutboxStatus.PUBLISHED));

        ConsumerRecord<String, String> record = topics.awaitRecord(Topics.ORDERS_CREATED, orderId);
        assertThat(record.key()).isEqualTo(String.valueOf(orderId));
        assertThat(new String(record.headers().lastHeader("correlationId").value(), StandardCharsets.UTF_8))
                .isEqualTo("cid-create-1");

        JsonNode event = topics.body(record);
        assertThat(event.get("eventType").asText()).isEqualTo("ORDER_CREATED");
        assertThat(event.get("eventVersion").asInt()).isEqualTo(1);
        assertThat(event.get("aggregateId").asText()).isEqualTo(String.valueOf(orderId));
        assertThat(event.get("correlationId").asText()).isEqualTo("cid-create-1");
        assertThat(event.get("source").asText()).isEqualTo("order-service");
        assertThat(payloadOf(event).get("totalAmount").decimalValue()).isEqualByComparingTo("199.79");
        assertThat(payloadOf(event).get("items")).hasSize(2);
    }

    @Test
    void pricesComeFromTheCatalogReplicaNotFromTheClient() throws Exception {
        long product = catalogProduct("10.00", true);

        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, userToken(uniqueId()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("totalAmount", 0.01, "items",
                                List.of(Map.of("productId", product, "quantity", 3, "unitPrice", 0.01))))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.totalAmount").value(30.0))
                .andExpect(jsonPath("$.items[0].unitPrice").value(10.0));
    }

    @Test
    void unknownOrInactiveProductsAreRejected() throws Exception {
        long inactive = catalogProduct("5.00", false);

        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, userToken(uniqueId()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("items", items(inactive, 1)))))
                .andExpect(status().isUnprocessableEntity());

        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, userToken(uniqueId()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("items", items(Long.MAX_VALUE, 1)))))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.message").value("Unknown product: " + Long.MAX_VALUE));
    }

    @Test
    void invalidRequestsAreRejected() throws Exception {
        mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, userToken(uniqueId()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("items", List.of()))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("items"));

        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("items", items(1, 1)))))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error").value("UNAUTHORIZED"));
    }

    @Test
    void usersOnlySeeTheirOwnOrdersAndAdminsSeeAll() throws Exception {
        long product = catalogProduct("7.50", true);
        long owner = uniqueId();
        long orderId = placeOrder(owner, "cid-access", product, 1);

        mockMvc.perform(get("/api/v1/orders/{id}", orderId).header(HttpHeaders.AUTHORIZATION, userToken(owner)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"));
        mockMvc.perform(get("/api/v1/orders/{id}", orderId).header(HttpHeaders.AUTHORIZATION, userToken(uniqueId())))
                .andExpect(status().isNotFound());
        mockMvc.perform(get("/api/v1/orders/{id}", orderId).header(HttpHeaders.AUTHORIZATION, adminToken()))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/orders").header(HttpHeaders.AUTHORIZATION, userToken(owner)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(1));
    }
}
