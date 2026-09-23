package com.example.commerce.platform.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

/**
 * Emite (auth-service) y valida (todos los servicios) JWT HS256.
 * Claims: {@code sub} = id de usuario, {@code email} y {@code role}.
 */
public class JwtTokenService {

    private static final Logger log = LoggerFactory.getLogger(JwtTokenService.class);
    private static final int MIN_SECRET_BYTES = 32;
    private static final String EMAIL_CLAIM = "email";
    private static final String ROLE_CLAIM = "role";

    private final SecretKey key;
    private final JwtProperties properties;
    private final Clock clock;

    public JwtTokenService(JwtProperties properties, Clock clock) {
        if (properties.secret() == null
                || properties.secret().getBytes(StandardCharsets.UTF_8).length < MIN_SECRET_BYTES) {
            throw new IllegalStateException("JWT_SECRET must be at least " + MIN_SECRET_BYTES + " bytes long");
        }
        this.key = Keys.hmacShaKeyFor(properties.secret().getBytes(StandardCharsets.UTF_8));
        this.properties = properties;
        this.clock = clock;
    }

    public String issue(AuthenticatedUser user) {
        Instant now = clock.instant();
        return Jwts.builder()
                .subject(String.valueOf(user.id()))
                .issuer(properties.issuer())
                .claim(EMAIL_CLAIM, user.email())
                .claim(ROLE_CLAIM, user.role().name())
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(properties.expiration())))
                .signWith(key, Jwts.SIG.HS256)
                .compact();
    }

    public long expirationSeconds() {
        return properties.expiration().toSeconds();
    }

    /** Valida firma, emisor y expiración. Nunca registra el token. */
    public Optional<AuthenticatedUser> parse(String token) {
        try {
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .requireIssuer(properties.issuer())
                    .clock(() -> Date.from(clock.instant()))
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();
            return Optional.of(new AuthenticatedUser(
                    Long.valueOf(claims.getSubject()),
                    claims.get(EMAIL_CLAIM, String.class),
                    Role.valueOf(claims.get(ROLE_CLAIM, String.class))));
        } catch (JwtException | IllegalArgumentException | NullPointerException ex) {
            log.debug("Rejected JWT: {}", ex.getClass().getSimpleName());
            return Optional.empty();
        }
    }
}
