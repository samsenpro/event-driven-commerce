package com.example.commerce.testing;

import com.example.commerce.platform.security.AuthenticatedUser;
import com.example.commerce.platform.security.JwtTokenService;
import com.example.commerce.platform.security.Role;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Base de los tests de integración de cada servicio:
 * <ul>
 *     <li>PostgreSQL y Kafka <b>reales</b> en contenedores, compartidos por todas las clases del módulo.</li>
 *     <li>La aplicación completa (REST + listeners + publicador del outbox).</li>
 *     <li>{@link TopicProbe} para leer lo que el servicio publica y {@link EventSender} para inyectarle
 *     los eventos que publicarían los demás servicios.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "commerce.security.jwt.secret=" + AbstractIntegrationTest.JWT_SECRET,
        "commerce.outbox.poll-interval=100ms",
        "commerce.outbox.initial-backoff=200ms",
        "commerce.kafka.retry.initial-interval=100ms",
        "commerce.kafka.retry.max-interval=200ms",
        "spring.kafka.listener.concurrency=1"
})
@AutoConfigureMockMvc
public abstract class AbstractIntegrationTest {

    public static final String JWT_SECRET = "integration-test-secret-key-with-at-least-32-bytes";

    @ServiceConnection
    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.9.1");

    /** Ids únicos entre tests para no depender de limpiar la base de datos o los topics. */
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis() % 1_000_000_000L);

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    private JwtTokenService jwtTokenService;

    protected TopicProbe topics;
    protected EventSender events;

    @BeforeEach
    void setUpKafkaHelpers() {
        topics = new TopicProbe(KAFKA.getBootstrapServers(), objectMapper);
        events = new EventSender(KAFKA.getBootstrapServers(), objectMapper);
    }

    protected static long uniqueId() {
        return IDS.incrementAndGet();
    }

    protected String bearer(long userId, Role role) {
        return "Bearer " + jwtTokenService.issue(new AuthenticatedUser(userId, "user" + userId + "@test.local", role));
    }

    protected String userToken(long userId) {
        return bearer(userId, Role.USER);
    }

    protected String adminToken() {
        return bearer(1L, Role.ADMIN);
    }

    protected String json(Object body) throws Exception {
        return objectMapper.writeValueAsString(body);
    }
}
