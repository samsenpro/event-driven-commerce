package com.example.commerce.inventory;

import com.example.commerce.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ProductIntegrationTest extends InventoryIntegrationTest {

    @Test
    void creatingAndUpdatingAProductPublishesItsFullState() throws Exception {
        long productId = createProduct("89.90", 7);

        JsonNode created = topics.body(topics.awaitRecord(Topics.PRODUCTS_CHANGED, productId));
        assertThat(created.get("eventType").asText()).isEqualTo("PRODUCT_CHANGED");
        assertThat(created.get("payload").get("price").decimalValue()).isEqualByComparingTo("89.90");

        mockMvc.perform(put("/api/v1/products/{id}", productId)
                        .header(HttpHeaders.AUTHORIZATION, adminToken())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("name", "Renamed", "price", new BigDecimal("79.90"), "active", false))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.active").value(false));

        assertThat(topics.awaitRecords(Topics.PRODUCTS_CHANGED, record -> String.valueOf(productId).equals(record.key()),
                2, Duration.ofSeconds(20)).get(1).value()).contains("\"price\":79.9", "\"active\":false");
    }

    @Test
    void usersCanBrowseButOnlyAdminsManageCatalogAndStock() throws Exception {
        long productId = createProduct("5.00", 3);

        mockMvc.perform(get("/api/v1/products/{id}", productId).header(HttpHeaders.AUTHORIZATION, userToken(uniqueId())))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/products")
                        .header(HttpHeaders.AUTHORIZATION, userToken(uniqueId()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("sku", "X-1", "name", "X", "price", 1, "initialStock", 1))))
                .andExpect(status().isForbidden());
        mockMvc.perform(get("/api/v1/inventory/{id}", productId).header(HttpHeaders.AUTHORIZATION, userToken(uniqueId())))
                .andExpect(status().isForbidden());
        mockMvc.perform(get("/api/v1/inventory/{id}", productId).header(HttpHeaders.AUTHORIZATION, adminToken()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(3));
    }

    @Test
    void adminAddsStockAndDuplicatedSkuIsRejected() throws Exception {
        long productId = createProduct("5.00", 3);

        mockMvc.perform(post("/api/v1/inventory/{id}/add", productId)
                        .header(HttpHeaders.AUTHORIZATION, adminToken())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("quantity", 7))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(10));

        String sku = "DUP-" + uniqueId();
        Map<String, Object> body = Map.of("sku", sku, "name", "Dup", "price", 1, "initialStock", 1);
        mockMvc.perform(post("/api/v1/products").header(HttpHeaders.AUTHORIZATION, adminToken())
                        .contentType(MediaType.APPLICATION_JSON).content(json(body)))
                .andExpect(status().isCreated());
        mockMvc.perform(post("/api/v1/products").header(HttpHeaders.AUTHORIZATION, adminToken())
                        .contentType(MediaType.APPLICATION_JSON).content(json(body)))
                .andExpect(status().isConflict());
    }
}
