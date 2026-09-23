package com.example.commerce.inventory;

import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.events.payload.OrderLine;
import com.example.commerce.inventory.dto.ProductDtos.StockResponse;
import com.example.commerce.inventory.entity.Reservation;
import com.example.commerce.inventory.entity.ReservationStatus;
import com.example.commerce.inventory.repository.ReservationRepository;
import com.example.commerce.inventory.service.StockService;
import com.example.commerce.testing.AbstractIntegrationTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

abstract class InventoryIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    protected StockService stockService;

    @Autowired
    protected ReservationRepository reservationRepository;

    protected long createProduct(String price, int stock) throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/products")
                        .header(HttpHeaders.AUTHORIZATION, adminToken())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("sku", "SKU-" + uniqueId(), "name", "Product", "price", new BigDecimal(price),
                                "initialStock", stock))))
                .andExpect(status().isCreated())
                .andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsString()).get("id").asLong();
    }

    protected static OrderCreated orderCreated(long orderId, Object... productIdAndQuantity) {
        List<OrderLine> lines = new ArrayList<>();
        BigDecimal total = BigDecimal.ZERO;
        for (int i = 0; i < productIdAndQuantity.length; i += 2) {
            int quantity = (Integer) productIdAndQuantity[i + 1];
            lines.add(new OrderLine((Long) productIdAndQuantity[i], "Product", quantity, BigDecimal.TEN));
            total = total.add(BigDecimal.TEN.multiply(BigDecimal.valueOf(quantity)));
        }
        return new OrderCreated(orderId, 99L, lines, total);
    }

    protected StockResponse stock(long productId) {
        return stockService.find(productId);
    }

    protected Reservation awaitReservation(long orderId, ReservationStatus expected) {
        await().atMost(Duration.ofSeconds(20)).until(() -> reservationRepository.findByOrderId(orderId)
                .map(Reservation::getStatus).orElse(null) == expected);
        return reservationRepository.findByOrderId(orderId).orElseThrow();
    }
}
