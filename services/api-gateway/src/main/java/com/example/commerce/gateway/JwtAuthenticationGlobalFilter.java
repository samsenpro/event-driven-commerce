package com.example.commerce.gateway;

import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

/**
 * Primera barrera de autenticación: rechaza en el borde (401) las peticiones sin un JWT válido
 * para no cargar a los servicios con tráfico anónimo. No toma decisiones de autorización: cada
 * servicio vuelve a validar el token y aplica sus propias reglas de rol y propiedad.
 */
@Component
public class JwtAuthenticationGlobalFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthenticationGlobalFilter.class);
    private static final String BEARER_PREFIX = "Bearer ";
    private static final int MIN_SECRET_BYTES = 32;
    private static final List<String> PUBLIC_PATHS = List.of(
            "/api/v1/auth/register", "/api/v1/auth/login",
            "/swagger-ui.html", "/swagger-ui/**", "/webjars/**", "/docs/**",
            "/actuator/health", "/actuator/health/**");

    private final JwtParser parser;
    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    public JwtAuthenticationGlobalFilter(GatewayJwtProperties properties) {
        byte[] secret = properties.secret() == null ? new byte[0] : properties.secret().getBytes(StandardCharsets.UTF_8);
        if (secret.length < MIN_SECRET_BYTES) {
            throw new IllegalStateException("JWT_SECRET must be at least " + MIN_SECRET_BYTES + " bytes long");
        }
        this.parser = Jwts.parser()
                .verifyWith(Keys.hmacShaKeyFor(secret))
                .requireIssuer(properties.issuer())
                .build();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (isPublic(path) || hasValidToken(exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION))) {
            return chain.filter(exchange);
        }
        return unauthorized(exchange, path);
    }

    private boolean isPublic(String path) {
        return PUBLIC_PATHS.stream().anyMatch(pattern -> pathMatcher.match(pattern, path));
    }

    private boolean hasValidToken(String header) {
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            return false;
        }
        try {
            parser.parseSignedClaims(header.substring(BEARER_PREFIX.length()).trim());
            return true;
        } catch (JwtException | IllegalArgumentException ex) {
            log.debug("Rejected JWT at the gateway: {}", ex.getClass().getSimpleName());
            return false;
        }
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange, String path) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().set(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
        String correlationId = response.getHeaders().getFirst(CorrelationIdGlobalFilter.HEADER);
        String body = """
                {"timestamp":"%s","status":401,"error":"UNAUTHORIZED","message":"Authentication required",\
                "path":"%s","correlationId":"%s"}""".formatted(Instant.now(), path, correlationId);
        DataBuffer buffer = response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(buffer));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}
