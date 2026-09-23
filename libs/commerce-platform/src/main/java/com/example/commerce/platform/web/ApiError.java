package com.example.commerce.platform.web;

import com.example.commerce.platform.correlation.CorrelationId;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.List;

/**
 * Formato único de error de todos los servicios.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        String correlationId,
        List<FieldViolation> errors
) {

    public static ApiError of(HttpStatus status, String message, String path, List<FieldViolation> errors) {
        return new ApiError(Instant.now(), status.value(), status.name(), message, path,
                CorrelationId.current().orElse(null), errors);
    }

    public record FieldViolation(String field, String message) {
    }
}
