package com.example.commerce.platform;

import com.example.commerce.events.Topics;
import com.example.commerce.platform.correlation.CorrelationIdFilter;
import com.example.commerce.platform.idempotency.IdempotentEventProcessor;
import com.example.commerce.platform.idempotency.ProcessedEventStore;
import com.example.commerce.platform.kafka.CommerceKafkaProperties;
import com.example.commerce.platform.kafka.KafkaErrorHandling;
import com.example.commerce.platform.messaging.EventFactory;
import com.example.commerce.platform.messaging.EventReader;
import com.example.commerce.platform.openapi.OpenApiSupport;
import com.example.commerce.platform.outbox.OutboxMaintenance;
import com.example.commerce.platform.outbox.OutboxProperties;
import com.example.commerce.platform.outbox.OutboxPublisher;
import com.example.commerce.platform.outbox.OutboxStore;
import com.example.commerce.platform.outbox.OutboxWriter;
import com.example.commerce.platform.security.CommerceSecurity;
import com.example.commerce.platform.security.JsonSecurityErrorHandler;
import com.example.commerce.platform.security.JwtAuthenticationFilter;
import com.example.commerce.platform.security.JwtProperties;
import com.example.commerce.platform.security.JwtTokenService;
import com.example.commerce.platform.web.GlobalExceptionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import jakarta.validation.Validator;
import org.apache.kafka.clients.admin.NewTopic;
import org.springdoc.core.customizers.OperationCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.stream.Stream;

/**
 * Infraestructura común que reciben todos los servicios al depender de {@code commerce-platform}.
 * El publicador del outbox solo se activa en los servicios que producen eventos
 * ({@code commerce.outbox.enabled=true}).
 */
@AutoConfiguration(after = {KafkaAutoConfiguration.class, JdbcTemplateAutoConfiguration.class})
@EnableConfigurationProperties({JwtProperties.class, CommerceKafkaProperties.class, OutboxProperties.class})
public class CommercePlatformAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Clock clock() {
        return Clock.systemUTC();
    }

    // --- Web ---

    @Bean
    public FilterRegistrationBean<CorrelationIdFilter> correlationIdFilter() {
        FilterRegistrationBean<CorrelationIdFilter> registration = new FilterRegistrationBean<>(new CorrelationIdFilter());
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    public GlobalExceptionHandler globalExceptionHandler() {
        return new GlobalExceptionHandler();
    }

    @Bean
    public OpenAPI commerceOpenApi(@Value("${commerce.openapi.title:${spring.application.name}}") String title,
                                   @Value("${commerce.openapi.description:}") String description,
                                   @Value("${commerce.openapi.version:1.0}") String version) {
        return OpenApiSupport.openApi(title, description, version);
    }

    @Bean
    public OperationCustomizer commonErrorResponses() {
        return OpenApiSupport.commonErrorResponses();
    }

    // --- Seguridad ---

    @Bean
    public JwtTokenService jwtTokenService(JwtProperties properties, Clock clock) {
        return new JwtTokenService(properties, clock);
    }

    @Bean
    public JwtAuthenticationFilter jwtAuthenticationFilter(JwtTokenService tokenService) {
        return new JwtAuthenticationFilter(tokenService);
    }

    /** El filtro JWT solo debe ejecutarse dentro de la cadena de Spring Security, no como filtro del servlet. */
    @Bean
    public FilterRegistrationBean<JwtAuthenticationFilter> jwtFilterRegistration(JwtAuthenticationFilter filter) {
        FilterRegistrationBean<JwtAuthenticationFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setEnabled(false);
        return registration;
    }

    @Bean
    public JsonSecurityErrorHandler jsonSecurityErrorHandler(ObjectMapper objectMapper) {
        return new JsonSecurityErrorHandler(objectMapper);
    }

    @Bean
    public CommerceSecurity commerceSecurity(JwtAuthenticationFilter filter, JsonSecurityErrorHandler errorHandler) {
        return new CommerceSecurity(filter, errorHandler);
    }

    // --- Persistencia: outbox e idempotencia (misma transacción que los datos de negocio) ---

    @Bean
    public ProcessedEventStore processedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }

    @Bean
    public OutboxStore outboxStore(JdbcTemplate jdbcTemplate) {
        return new OutboxStore(jdbcTemplate);
    }

    // --- Mensajería ---

    @Bean
    public EventFactory eventFactory(Clock clock, @Value("${spring.application.name}") String serviceName) {
        return new EventFactory(clock, serviceName);
    }

    @Bean
    public EventReader eventReader(ObjectMapper objectMapper, Validator validator) {
        return new EventReader(objectMapper, validator);
    }

    @Bean
    public OutboxWriter outboxWriter(OutboxStore store, EventFactory eventFactory, ObjectMapper objectMapper,
                                     Clock clock) {
        return new OutboxWriter(store, eventFactory, objectMapper, clock);
    }

    @Bean
    public IdempotentEventProcessor idempotentEventProcessor(ProcessedEventStore store,
                                                             TransactionTemplate transactionTemplate, Clock clock,
                                                             @Value("${spring.application.name}") String serviceName) {
        return new IdempotentEventProcessor(store, transactionTemplate, clock, serviceName);
    }

    /** Solo en los servicios que usan Kafka (auth-service la excluye). */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(KafkaTemplate.class)
    static class KafkaConfiguration {

        /** Usado por la factoría de listeners de Spring Boot: retry con backoff y DLT. */
        @Bean
        CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                             CommerceKafkaProperties properties) {
            return KafkaErrorHandling.errorHandler(kafkaTemplate, properties.retry());
        }

        /**
         * Declara todos los topics y sus DLT. {@link KafkaAdmin} solo crea los que no existen, así que
         * todos los servicios pueden declararlos sin conflicto.
         */
        @Bean
        KafkaAdmin.NewTopics commerceTopics(CommerceKafkaProperties properties) {
            return new KafkaAdmin.NewTopics(Topics.ALL.stream()
                    .flatMap(topic -> Stream.of(topic, Topics.deadLetterOf(topic)))
                    .map(topic -> TopicBuilder.name(topic)
                            .partitions(properties.partitions())
                            .replicas(properties.replicationFactor())
                            .build())
                    .toArray(NewTopic[]::new));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(KafkaTemplate.class)
    @ConditionalOnProperty(prefix = "commerce.outbox", name = "enabled", havingValue = "true")
    @EnableScheduling
    static class OutboxPublishingConfiguration {

        @Bean
        OutboxPublisher outboxPublisher(OutboxStore store, KafkaTemplate<String, String> kafkaTemplate,
                                        TransactionTemplate transactionTemplate, OutboxProperties properties,
                                        Clock clock) {
            return new OutboxPublisher(store, kafkaTemplate, transactionTemplate, properties, clock);
        }

        @Bean
        OutboxMaintenance outboxMaintenance(OutboxStore store, OutboxProperties properties, Clock clock) {
            return new OutboxMaintenance(store, properties, clock);
        }
    }
}
