package com.example.commerce.platform.outbox;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxPropertiesTest {

    @Test
    void appliesSensibleDefaults() {
        OutboxProperties properties = new OutboxProperties(true, null, 0, 0, null, null, null);

        assertThat(properties.maxAttempts()).isEqualTo(3);
        assertThat(properties.batchSize()).isEqualTo(50);
        assertThat(properties.retention()).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void backoffGrowsExponentially() {
        OutboxProperties properties = new OutboxProperties(true, null, 10, 3, Duration.ofSeconds(2), null, null);

        assertThat(properties.backoffAfter(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(properties.backoffAfter(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(properties.backoffAfter(3)).isEqualTo(Duration.ofSeconds(8));
    }
}
