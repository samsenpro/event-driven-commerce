package com.example.commerce.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Cliente de la plataforma para los tests E2E: HTTP a través del API Gateway y lectura directa de Kafka.
 * Configuración por variables de entorno (valores por defecto para docker compose en local).
 */
final class Platform {

    static final String BASE_URL = env("E2E_BASE_URL", "http://localhost:8080");
    static final String KAFKA = env("E2E_KAFKA_BOOTSTRAP", "localhost:9094");
    static final String ADMIN_EMAIL = env("E2E_ADMIN_EMAIL", "admin@demo.local");
    static final String ADMIN_PASSWORD = env("E2E_ADMIN_PASSWORD", "ChangeMe123");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private Platform() {
    }

    record Response(int status, JsonNode body, HttpResponse<String> raw) {
    }

    static Response request(String method, String path, Object body, String token, String correlationId) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(BASE_URL + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(toJson(body)));
        if (token != null) {
            builder.header("Authorization", "Bearer " + token);
        }
        if (correlationId != null) {
            builder.header("X-Correlation-ID", correlationId);
        }
        try {
            HttpResponse<String> response = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode json = response.body() == null || response.body().isBlank() ? null : JSON.readTree(response.body());
            return new Response(response.statusCode(), json, response);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        }
    }

    static Response get(String path, String token) {
        return request("GET", path, null, token, null);
    }

    static String login(String email, String password) {
        Response response = request("POST", "/api/v1/auth/login", Map.of("email", email, "password", password), null, null);
        if (response.status() != 200) {
            throw new IllegalStateException("Login failed for " + email + ": " + response.status());
        }
        return response.body().get("accessToken").asText();
    }

    static String newCustomer() {
        String email = "e2e-" + UUID.randomUUID() + "@test.local";
        request("POST", "/api/v1/auth/register", Map.of("name", "E2E", "email", email, "password", "Password123"),
                null, null);
        return login(email, "Password123");
    }

    /**
     * Todos los mensajes con esa clave (orderId) en los topics indicados. Lee las particiones
     * directamente ({@code assign}), sin consumer group, para no dejar grupos residuales en Kafka.
     */
    static List<ConsumerRecord<String, String>> eventsFor(String key, List<String> topics, Duration window) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            List<TopicPartition> partitions = topics.stream()
                    .flatMap(topic -> consumer.partitionsFor(topic).stream()
                            .map(info -> new TopicPartition(topic, info.partition())))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Instant deadline = Instant.now().plus(window);
            while (Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofMillis(300)).forEach(record -> {
                    if (key.equals(record.key())) {
                        matches.add(record);
                    }
                });
            }
        }
        return matches;
    }

    static JsonNode parse(String json) {
        try {
            return JSON.readTree(json);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static String toJson(Object body) {
        try {
            return JSON.writeValueAsString(body);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
