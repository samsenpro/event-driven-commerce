package com.example.commerce.order;

import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.platform.outbox.OutboxStore;
import com.example.commerce.platform.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * El pedido y su evento se confirman o se deshacen juntos: nunca hay un pedido sin evento ni un
 * evento de un pedido que no existe.
 */
class OutboxAtomicityIntegrationTest extends OrderServiceIntegrationTest {

    @MockitoSpyBean
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxStore outboxStore;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void failingToWriteTheEventRollsBackTheOrder() throws Exception {
        long product = catalogProduct("12.00", true);
        long userId = uniqueId();
        doThrow(new IllegalStateException("outbox unavailable")).when(outboxWriter).publish(any(), any());

        try {
            mockMvc.perform(post("/api/v1/orders")
                            .header(HttpHeaders.AUTHORIZATION, userToken(userId))
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(json(Map.of("items", items(product, 1)))))
                    .andExpect(status().isInternalServerError());
        } finally {
            doCallRealMethod().when(outboxWriter).publish(any(), any());
        }

        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = ?", Long.class, userId))
                .isZero();
    }

    @Test
    void failingBusinessWriteRollsBackTheOutboxEvent() {
        long orderId = uniqueId();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            outboxWriter.publish(new OrderCancelled(orderId, 1L, "TEST"), orderId);
            // Insert inválido (user_id NOT NULL) en la misma transacción
            jdbcTemplate.update("INSERT INTO orders (user_id, status, total_amount, created_at, updated_at) "
                    + "VALUES (NULL, 'PENDING', 1, now(), now())");
        })).isInstanceOf(DataIntegrityViolationException.class);

        assertThat(outboxStore.findByAggregate("Order", String.valueOf(orderId))).isEmpty();
    }

    @Test
    void successfulTransactionCommitsBoth() throws Exception {
        long product = catalogProduct("12.00", true);

        long orderId = placeOrder(uniqueId(), "atomic-ok", product, 1);

        assertThat(orderRepository.existsById(orderId)).isTrue();
        assertThat(outboxStore.findByAggregate("Order", String.valueOf(orderId))).hasSize(1);
    }

    @Test
    void writingToTheOutboxOutsideATransactionIsRejected() {
        assertThatThrownBy(() -> outboxWriter.publish(new OrderCancelled(1L, 1L, "TEST"), 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inside the business transaction");
    }
}
