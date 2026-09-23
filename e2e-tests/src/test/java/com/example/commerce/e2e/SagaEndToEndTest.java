package com.example.commerce.e2e;

import com.example.commerce.e2e.Platform.Response;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.example.commerce.e2e.Platform.get;
import static com.example.commerce.e2e.Platform.request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * La saga completa a través de todos los servicios reales, Kafka y PostgreSQL:
 * <ul>
 *     <li>flujo exitoso: creado → stock reservado → pago aprobado → confirmado → enviado,</li>
 *     <li>compensación: pago rechazado → pedido cancelado → stock liberado,</li>
 *     <li>sin stock: pedido cancelado sin tocar el pago.</li>
 * </ul>
 */
class SagaEndToEndTest {

    private static final Duration SAGA_TIMEOUT = Duration.ofSeconds(60);
    private static final List<String> ORDER_TOPICS = List.of("orders.created", "inventory.reserved",
            "payments.requested", "payments.approved", "orders.confirmed", "shipments.created",
            "payments.rejected", "orders.cancelled", "inventory.released", "inventory.failed");

    private static String admin;

    @BeforeAll
    static void loginAsAdmin() {
        admin = Platform.login(Platform.ADMIN_EMAIL, Platform.ADMIN_PASSWORD);
    }

    @Test
    void successfulOrderIsReservedPaidConfirmedAndShipped() {
        long product = createProduct("25.00", 10);
        String customer = Platform.newCustomer();
        String correlationId = "e2e-ok-" + UUID.randomUUID();

        long orderId = placeOrder(customer, product, 2, correlationId);

        JsonNode order = awaitOrderStatus(customer, orderId, "SHIPPED");
        assertThat(order.get("totalAmount").decimalValue()).isEqualByComparingTo("50.00");
        await().atMost(SAGA_TIMEOUT).untilAsserted(() ->
                assertThat(stock(product)).containsEntry("available", 8).containsEntry("reserved", 0));
        assertThat(get("/api/v1/payments/" + orderId, admin).body().get("status").asText()).isEqualTo("APPROVED");
        assertThat(get("/api/v1/shipments/" + orderId, admin).body().get("trackingNumber").asText()).startsWith("TRK-");
        await().atMost(SAGA_TIMEOUT).untilAsserted(() -> assertThat(notificationTypes(orderId))
                .contains("ORDER_CREATED", "PAYMENT_APPROVED", "ORDER_SHIPPED"));

        // Todos los eventos del pedido comparten el correlation ID de la petición original
        List<ConsumerRecord<String, String>> events =
                Platform.eventsFor(String.valueOf(orderId), ORDER_TOPICS, Duration.ofSeconds(5));
        assertThat(events).extracting(ConsumerRecord::topic).contains("orders.created", "inventory.reserved",
                "payments.requested", "payments.approved", "orders.confirmed", "shipments.created");
        assertThat(events).allSatisfy(record ->
                assertThat(Platform.parse(record.value()).get("correlationId").asText()).isEqualTo(correlationId));
    }

    @Test
    void rejectedPaymentCancelsTheOrderAndReleasesTheStock() {
        // 2 x 600 = 1200 > límite de aprobación del simulador (1000)
        long product = createProduct("600.00", 5);
        String customer = Platform.newCustomer();

        long orderId = placeOrder(customer, product, 2, "e2e-rejected-" + UUID.randomUUID());

        JsonNode order = awaitOrderStatus(customer, orderId, "CANCELLED");
        assertThat(order.get("cancellationReason").asText()).isEqualTo("PAYMENT_REJECTED");
        await().atMost(SAGA_TIMEOUT).untilAsserted(() ->
                assertThat(stock(product)).containsEntry("available", 5).containsEntry("reserved", 0));
        assertThat(get("/api/v1/payments/" + orderId, admin).body().get("status").asText()).isEqualTo("REJECTED");
        await().atMost(SAGA_TIMEOUT).untilAsserted(() ->
                assertThat(notificationTypes(orderId)).contains("PAYMENT_REJECTED", "ORDER_CANCELLED"));
        assertThat(Platform.eventsFor(String.valueOf(orderId), ORDER_TOPICS, Duration.ofSeconds(5)))
                .extracting(ConsumerRecord::topic)
                // Topics distintos no garantizan orden entre sí: se comprueba qué ocurrió, no la secuencia
                .contains("orders.created", "inventory.reserved", "payments.requested", "payments.rejected",
                        "orders.cancelled", "inventory.released");
    }

    @Test
    void orderWithoutEnoughStockIsCancelledWithoutCharging() {
        long product = createProduct("10.00", 1);
        String customer = Platform.newCustomer();

        long orderId = placeOrder(customer, product, 3, "e2e-no-stock-" + UUID.randomUUID());

        assertThat(awaitOrderStatus(customer, orderId, "CANCELLED").get("cancellationReason").asText())
                .isEqualTo("OUT_OF_STOCK");
        assertThat(stock(product)).containsEntry("available", 1).containsEntry("reserved", 0);
        // Nunca se cobra: payment solo registra un marcador VOIDED al recibir la cancelación
        await().atMost(SAGA_TIMEOUT).untilAsserted(() -> assertThat(
                get("/api/v1/payments/" + orderId, admin).body().get("status").asText()).isEqualTo("VOIDED"));
    }

    private static long createProduct(String price, int stock) {
        Response created = request("POST", "/api/v1/products", Map.of("sku", "E2E-" + UUID.randomUUID().toString()
                .substring(0, 8), "name", "E2E product", "price", price, "initialStock", stock), admin, null);
        assertThat(created.status()).isEqualTo(201);
        return created.body().get("id").asLong();
    }

    /**
     * El order-service conoce el producto cuando procesa PRODUCT_CHANGED (consistencia eventual): hasta
     * entonces responde 422, así que se reintenta brevemente.
     */
    private static long placeOrder(String token, long productId, int quantity, String correlationId) {
        AtomicReference<Response> accepted = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(300)).until(() -> {
            Response response = request("POST", "/api/v1/orders",
                    Map.of("items", List.of(Map.of("productId", productId, "quantity", quantity))), token, correlationId);
            accepted.set(response);
            return response.status() == 202;
        });
        assertThat(accepted.get().raw().headers().firstValue("X-Correlation-ID")).contains(correlationId);
        assertThat(accepted.get().body().get("status").asText()).isEqualTo("PENDING");
        return accepted.get().body().get("id").asLong();
    }

    private static JsonNode awaitOrderStatus(String token, long orderId, String expected) {
        AtomicReference<JsonNode> order = new AtomicReference<>();
        await().atMost(SAGA_TIMEOUT).pollInterval(Duration.ofMillis(500)).until(() -> {
            order.set(get("/api/v1/orders/" + orderId, token).body());
            return expected.equals(order.get().get("status").asText());
        });
        return order.get();
    }

    private static Map<String, Integer> stock(long productId) {
        JsonNode stock = get("/api/v1/inventory/" + productId, admin).body();
        return Map.of("available", stock.get("available").asInt(), "reserved", stock.get("reserved").asInt());
    }

    private static List<String> notificationTypes(long orderId) {
        JsonNode notifications = get("/api/v1/notifications?orderId=" + orderId, admin).body();
        List<String> types = new ArrayList<>();
        notifications.forEach(notification -> types.add(notification.get("type").asText()));
        return types;
    }
}
