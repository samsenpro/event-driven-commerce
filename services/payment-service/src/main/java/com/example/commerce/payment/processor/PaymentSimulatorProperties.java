package com.example.commerce.payment.processor;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

/**
 * @param approvalLimit        importes superiores se rechazan (simula fondos insuficientes)
 * @param transientFailureRate probabilidad [0, 1] de un fallo transitorio de la pasarela
 */
@ConfigurationProperties(prefix = "commerce.payment.simulator")
public record PaymentSimulatorProperties(BigDecimal approvalLimit, double transientFailureRate) {

    private static final BigDecimal DEFAULT_APPROVAL_LIMIT = new BigDecimal("1000.00");

    public PaymentSimulatorProperties {
        approvalLimit = approvalLimit == null ? DEFAULT_APPROVAL_LIMIT : approvalLimit;
        if (transientFailureRate < 0 || transientFailureRate > 1) {
            throw new IllegalArgumentException("transient-failure-rate must be between 0 and 1");
        }
    }
}
