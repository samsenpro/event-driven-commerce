package com.example.commerce.gateway;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El gateway contra un servicio de destino falso (MockWebServer): enrutado, validación JWT en el
 * borde y propagación del correlation ID.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "gateway.jwt.secret=" + ApiGatewayIntegrationTest.SECRET)
@AutoConfigureWebTestClient
class ApiGatewayIntegrationTest {

    static final String SECRET = "gateway-test-secret-key-with-at-least-32-bytes";
    private static final MockWebServer BACKEND = new MockWebServer();

    @Autowired
    private WebTestClient client;

    @DynamicPropertySource
    static void routes(DynamicPropertyRegistry registry) throws IOException {
        BACKEND.start();
        String url = BACKEND.url("/").toString().replaceAll("/$", "");
        for (String service : new String[]{"AUTH", "ORDER", "INVENTORY", "PAYMENT", "NOTIFICATION", "SHIPPING"}) {
            registry.add(service + "_SERVICE_URL", () -> url);
        }
    }

    @AfterAll
    static void stopBackend() throws IOException {
        BACKEND.shutdown();
    }

    @Test
    void requestWithoutTokenIsRejectedAtTheEdge() {
        int before = BACKEND.getRequestCount();

        client.get().uri("/api/v1/orders")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().exists("X-Correlation-ID")
                .expectBody().jsonPath("$.error").isEqualTo("UNAUTHORIZED");

        assertThat(BACKEND.getRequestCount()).isEqualTo(before);
    }

    @Test
    void invalidTokenIsRejectedAtTheEdge() {
        client.get().uri("/api/v1/orders")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token("another-secret-key-with-at-least-32-bytes!!"))
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void validTokenIsRoutedWithAuthorizationAndCorrelationId() throws InterruptedException {
        BACKEND.enqueue(new MockResponse().setResponseCode(200).setBody("[]"));
        String token = token(SECRET);

        client.get().uri("/api/v1/orders")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header("X-Correlation-ID", "edge-123")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Correlation-ID", "edge-123");

        RecordedRequest forwarded = BACKEND.takeRequest(2, TimeUnit.SECONDS);
        assertThat(forwarded.getPath()).isEqualTo("/api/v1/orders");
        assertThat(forwarded.getHeader(HttpHeaders.AUTHORIZATION)).isEqualTo("Bearer " + token);
        assertThat(forwarded.getHeader("X-Correlation-ID")).isEqualTo("edge-123");
    }

    @Test
    void loginIsPublicAndGetsAGeneratedCorrelationId() throws InterruptedException {
        BACKEND.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        client.post().uri("/api/v1/auth/login")
                .bodyValue("{}")
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .exchange()
                .expectStatus().isOk();

        RecordedRequest forwarded = BACKEND.takeRequest(2, TimeUnit.SECONDS);
        assertThat(forwarded.getPath()).isEqualTo("/api/v1/auth/login");
        assertThat(forwarded.getHeader("X-Correlation-ID")).matches("[0-9a-f-]{36}");
    }

    @Test
    void serviceDocumentationIsExposedThroughTheGateway() throws InterruptedException {
        BACKEND.enqueue(new MockResponse().setResponseCode(200).setBody("{\"openapi\":\"3.1.0\"}"));

        client.get().uri("/docs/orders/v3/api-docs")
                .exchange()
                .expectStatus().isOk();

        assertThat(BACKEND.takeRequest(2, TimeUnit.SECONDS).getPath()).isEqualTo("/v3/api-docs");
    }

    private static String token(String secret) {
        return Jwts.builder()
                .subject("7")
                .issuer("event-driven-commerce")
                .claim("role", "USER")
                .expiration(Date.from(Instant.now().plusSeconds(300)))
                .signWith(Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8)))
                .compact();
    }
}
