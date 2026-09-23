package com.example.commerce.platform.security;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtTokenServiceTest {

    private static final String SECRET = "unit-test-secret-key-with-at-least-32-bytes";
    private static final Instant NOW = Instant.parse("2026-09-23T10:00:00Z");
    private static final AuthenticatedUser USER = new AuthenticatedUser(7L, "jane@example.com", Role.ADMIN);

    @Test
    void issuedTokenRoundTripsTheIdentity() {
        JwtTokenService service = service(SECRET, NOW);

        assertThat(service.parse(service.issue(USER))).contains(USER);
        assertThat(service.expirationSeconds()).isEqualTo(3600);
    }

    @Test
    void expiredTokenIsRejected() {
        String token = service(SECRET, NOW).issue(USER);

        assertThat(service(SECRET, NOW.plus(Duration.ofHours(2))).parse(token)).isEmpty();
    }

    @Test
    void tokenSignedWithAnotherKeyIsRejected() {
        String token = service("another-unit-test-secret-with-32-bytes!!", NOW).issue(USER);

        assertThat(service(SECRET, NOW).parse(token)).isEmpty();
    }

    @Test
    void garbageIsRejected() {
        assertThat(service(SECRET, NOW).parse("not-a-jwt")).isEmpty();
    }

    @Test
    void shortSecretFailsFast() {
        assertThatThrownBy(() -> service("short", NOW)).isInstanceOf(IllegalStateException.class);
    }

    private static JwtTokenService service(String secret, Instant now) {
        return new JwtTokenService(new JwtProperties(secret, null, null), Clock.fixed(now, ZoneOffset.UTC));
    }
}
