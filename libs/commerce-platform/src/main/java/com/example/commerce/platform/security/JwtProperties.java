package com.example.commerce.platform.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @param secret     clave HMAC compartida (al menos 32 bytes)
 * @param issuer     emisor que se firma y se exige al validar
 * @param expiration validez de los tokens emitidos
 */
@ConfigurationProperties(prefix = "commerce.security.jwt")
public record JwtProperties(String secret, String issuer, Duration expiration) {

    private static final String DEFAULT_ISSUER = "event-driven-commerce";
    private static final Duration DEFAULT_EXPIRATION = Duration.ofHours(1);

    public JwtProperties {
        issuer = issuer == null ? DEFAULT_ISSUER : issuer;
        expiration = expiration == null ? DEFAULT_EXPIRATION : expiration;
    }

    @Override
    public String toString() {
        return "JwtProperties{issuer=" + issuer + ", expiration=" + expiration + "}";
    }
}
