package com.example.commerce.platform.correlation;

import org.slf4j.MDC;

import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Correlation ID de la operación en curso. Nace en la petición HTTP (o en el gateway), viaja en
 * el {@code EventEnvelope} de cada evento y los consumidores lo restauran en el MDC, así que todos
 * los logs y eventos de una misma saga comparten el mismo valor.
 */
public final class CorrelationId {

    public static final String HEADER = "X-Correlation-ID";
    public static final String MDC_KEY = "correlationId";

    /** Solo IDs cortos y seguros: evita inyección en logs y cabeceras enormes. */
    private static final Pattern VALID = Pattern.compile("^[A-Za-z0-9._-]{1,64}$");

    private CorrelationId() {
    }

    public static Optional<String> current() {
        return Optional.ofNullable(MDC.get(MDC_KEY));
    }

    public static String currentOrNew() {
        return current().orElseGet(CorrelationId::generate);
    }

    public static String sanitizeOrGenerate(String candidate) {
        return candidate != null && VALID.matcher(candidate).matches() ? candidate : generate();
    }

    public static String generate() {
        return UUID.randomUUID().toString();
    }
}
