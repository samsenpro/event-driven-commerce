package com.example.commerce.order;

import com.example.commerce.events.payload.ProductChanged;
import com.example.commerce.order.entity.Order;
import com.example.commerce.order.entity.OrderStatus;
import com.example.commerce.order.repository.CatalogProductRepository;
import com.example.commerce.order.repository.OrderRepository;
import com.example.commerce.testing.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Utilidades comunes de los tests del order-service.
 */
abstract class OrderServiceIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    protected OrderRepository orderRepository;

    @Autowired
    protected CatalogProductRepository catalogRepository;

    /** Publica un producto en products.changed (como haría inventory-service) y espera a la réplica. */
    protected long catalogProduct(String price, boolean active) {
        long productId = uniqueId();
        events.publish(new ProductChanged(productId, "SKU-" + productId, "Product " + productId,
                new BigDecimal(price), active, Instant.now()), productId);
        await().atMost(Duration.ofSeconds(20)).until(() -> catalogRepository.existsById(productId));
        return productId;
    }

    protected long placeOrder(long userId, String correlationId, Object... productIdAndQuantity) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/orders")
                        .header(HttpHeaders.AUTHORIZATION, userToken(userId))
                        .header("X-Correlation-ID", correlationId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("items", items(productIdAndQuantity)))))
                .andExpect(status().isAccepted())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsString()).get("id").asLong();
    }

    protected static List<Map<String, Object>> items(Object... productIdAndQuantity) {
        List<Map<String, Object>> items = new java.util.ArrayList<>();
        for (int i = 0; i < productIdAndQuantity.length; i += 2) {
            items.add(Map.of("productId", productIdAndQuantity[i], "quantity", productIdAndQuantity[i + 1]));
        }
        return items;
    }

    protected Order awaitStatus(long orderId, OrderStatus expected) {
        await().atMost(Duration.ofSeconds(20))
                .until(() -> orderRepository.findById(orderId).map(Order::getStatus).orElse(null) == expected);
        return orderRepository.findById(orderId).orElseThrow();
    }

    protected JsonNode payloadOf(JsonNode envelope) {
        return envelope.get("payload");
    }
}
