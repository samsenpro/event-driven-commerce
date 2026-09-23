package com.example.commerce.platform.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @param partitions        particiones de cada topic (y de su DLT)
 * @param replicationFactor réplicas de cada topic (1 en local; 3 en un clúster real)
 * @param retry             reintentos de los consumidores ante errores transitorios
 */
@ConfigurationProperties(prefix = "commerce.kafka")
public record CommerceKafkaProperties(int partitions, short replicationFactor, Retry retry) {

    private static final int DEFAULT_PARTITIONS = 3;

    public CommerceKafkaProperties {
        partitions = partitions <= 0 ? DEFAULT_PARTITIONS : partitions;
        replicationFactor = replicationFactor <= 0 ? 1 : replicationFactor;
        retry = retry == null ? new Retry(0, null, 0, null) : retry;
    }

    /**
     * @param maxRetries      reintentos tras el primer intento (nunca infinitos)
     * @param initialInterval espera antes del primer reintento
     * @param multiplier      factor de crecimiento del backoff
     * @param maxInterval     espera máxima entre reintentos
     */
    public record Retry(int maxRetries, Duration initialInterval, double multiplier, Duration maxInterval) {

        public Retry {
            maxRetries = maxRetries <= 0 ? 3 : maxRetries;
            initialInterval = initialInterval == null ? Duration.ofMillis(500) : initialInterval;
            multiplier = multiplier <= 1 ? 2.0 : multiplier;
            maxInterval = maxInterval == null ? Duration.ofSeconds(5) : maxInterval;
        }
    }
}
