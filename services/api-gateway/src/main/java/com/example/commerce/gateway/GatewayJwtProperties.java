package com.example.commerce.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.jwt")
public record GatewayJwtProperties(String secret, String issuer) {

    @Override
    public String toString() {
        return "GatewayJwtProperties{issuer=" + issuer + "}";
    }
}
