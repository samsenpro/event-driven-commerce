package com.example.commerce.payment.processor;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimulatedPaymentProcessorTest {

    private static final RandomGenerator NEVER_FAILS = () -> Long.MAX_VALUE;

    @Test
    void approvesAmountsUpToTheLimit() {
        var processor = new SimulatedPaymentProcessor(
                new PaymentSimulatorProperties(new BigDecimal("100.00"), 0.0), fixed(0.99));

        assertThat(processor.charge(1L, 1L, new BigDecimal("100.00")).approved()).isTrue();
    }

    @Test
    void rejectsAmountsAboveTheLimit() {
        var processor = new SimulatedPaymentProcessor(
                new PaymentSimulatorProperties(new BigDecimal("100.00"), 0.0), fixed(0.99));

        var result = processor.charge(1L, 1L, new BigDecimal("100.01"));

        assertThat(result.approved()).isFalse();
        assertThat(result.reason()).contains("approval limit");
    }

    @Test
    void simulatesTransientGatewayFailures() {
        var processor = new SimulatedPaymentProcessor(
                new PaymentSimulatorProperties(new BigDecimal("100.00"), 0.5), fixed(0.1));

        assertThatThrownBy(() -> processor.charge(1L, 1L, BigDecimal.ONE))
                .isInstanceOf(PaymentGatewayUnavailableException.class);
    }

    @Test
    void rejectsInvalidFailureRate() {
        assertThatThrownBy(() -> new PaymentSimulatorProperties(BigDecimal.ONE, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RandomGenerator fixed(double value) {
        return new RandomGenerator() {
            @Override
            public long nextLong() {
                return NEVER_FAILS.nextLong();
            }

            @Override
            public double nextDouble() {
                return value;
            }
        };
    }
}
