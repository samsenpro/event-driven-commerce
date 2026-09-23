package com.example.commerce.notification;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.events.payload.OrderLine;
import com.example.commerce.events.payload.PaymentRejected;
import com.example.commerce.events.payload.ShipmentCreated;
import com.example.commerce.notification.entity.Notification;
import com.example.commerce.notification.entity.NotificationType;
import com.example.commerce.notification.repository.NotificationRepository;
import com.example.commerce.notification.service.NotificationService;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import com.example.commerce.testing.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class NotificationIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private NotificationRepository repository;

    @Autowired
    private IdempotentEventProcessor processor;

    @Autowired
    private NotificationService notificationService;

    @Test
    void sagaEventsBecomeCustomerNotifications() throws Exception {
        long orderId = uniqueId();

        events.publish(orderCreated(orderId), orderId);
        events.publish(new PaymentRejected(orderId, 5L, UUID.randomUUID(), "Insufficient funds"), orderId);

        List<Notification> sent = awaitNotifications(orderId, 2);
        assertThat(sent).extracting(Notification::getType)
                .containsExactlyInAnyOrder(NotificationType.ORDER_CREATED, NotificationType.PAYMENT_REJECTED);
        mockMvc.perform(get("/api/v1/notifications").param("orderId", String.valueOf(orderId))
                        .header(HttpHeaders.AUTHORIZATION, adminToken()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void shipmentProducesOrderShippedNotification() {
        long orderId = uniqueId();

        events.publish(new ShipmentCreated(orderId, 5L, UUID.randomUUID(), "TRK-42"), orderId);

        assertThat(awaitNotifications(orderId, 1).getFirst().getMessage()).contains("TRK-42");
    }

    @Test
    void duplicatedEventSendsASingleNotification() {
        long orderId = uniqueId();
        EventEnvelope<OrderCreated> event = events.envelope(orderCreated(orderId), orderId, "dup");

        events.send(event);
        events.send(event);

        awaitNotifications(orderId, 1);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .until(() -> repository.findAllByOrderIdOrderBySentAtAsc(orderId).size() == 1);
    }

    @Test
    void concurrentDuplicatesSendASingleNotification() throws Exception {
        long orderId = uniqueId();
        EventEnvelope<OrderCreated> event = events.envelope(orderCreated(orderId), orderId, "race");
        CountDownLatch start = new CountDownLatch(1);
        Callable<Boolean> delivery = () -> {
            start.await();
            return processor.process("order-created", event, notificationService::onOrderCreated);
        };

        List<Boolean> results = new ArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(3)) {
            List<Future<Boolean>> futures = List.of(executor.submit(delivery), executor.submit(delivery),
                    executor.submit(delivery));
            start.countDown();
            for (Future<Boolean> future : futures) {
                results.add(future.get());
            }
        }

        assertThat(results).containsExactlyInAnyOrder(true, false, false);
        assertThat(repository.findAllByOrderIdOrderBySentAtAsc(orderId)).hasSize(1);
    }

    @Test
    void onlyAdminsCanReadNotifications() throws Exception {
        mockMvc.perform(get("/api/v1/notifications").param("orderId", "1")
                        .header(HttpHeaders.AUTHORIZATION, userToken(uniqueId())))
                .andExpect(status().isForbidden());
    }

    private static OrderCreated orderCreated(long orderId) {
        return new OrderCreated(orderId, 5L, List.of(new OrderLine(1L, "Keyboard", 1, new BigDecimal("50.00"))),
                new BigDecimal("50.00"));
    }

    private List<Notification> awaitNotifications(long orderId, int expected) {
        await().atMost(Duration.ofSeconds(20))
                .until(() -> repository.findAllByOrderIdOrderBySentAtAsc(orderId).size() >= expected);
        return repository.findAllByOrderIdOrderBySentAtAsc(orderId);
    }
}
