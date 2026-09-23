package com.example.commerce.platform.openapi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.examples.Example;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.customizers.OperationCustomizer;

import java.util.List;
import java.util.Map;

/**
 * Documentación OpenAPI común: autenticación Bearer, servidor relativo (las peticiones de
 * "Try it out" pasan por el API Gateway) y ejemplos de error con el formato real de {@code ApiError}.
 */
public final class OpenApiSupport {

    public static final String BEARER_SCHEME = "bearerAuth";
    private static final String JSON = "application/json";

    private static final Map<String, String[]> COMMON_ERRORS = Map.of(
            "400", new String[]{"Validation failed", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":400,"error":"BAD_REQUEST","message":"Validation failed",\
                    "path":"/api/v1/orders","correlationId":"abc-123","errors":[{"field":"items","message":"must not be empty"}]}"""},
            "401", new String[]{"Missing, invalid or expired JWT", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":401,"error":"UNAUTHORIZED","message":"Authentication required",\
                    "path":"/api/v1/orders","correlationId":"abc-123"}"""},
            "403", new String[]{"Authenticated but not allowed", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":403,"error":"FORBIDDEN","message":"Access denied",\
                    "path":"/api/v1/products","correlationId":"abc-123"}"""},
            "404", new String[]{"Resource not found", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":404,"error":"NOT_FOUND","message":"Order not found: 42",\
                    "path":"/api/v1/orders/42","correlationId":"abc-123"}"""},
            "409", new String[]{"Conflict with the current state", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":409,"error":"CONFLICT","message":"SKU already exists: KB-001",\
                    "path":"/api/v1/products","correlationId":"abc-123"}"""},
            "422", new String[]{"Business rule violated", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":422,"error":"UNPROCESSABLE_ENTITY",\
                    "message":"Order 42 cannot be cancelled in status SHIPPED","path":"/api/v1/orders/42/cancel","correlationId":"abc-123"}"""},
            "500", new String[]{"Unexpected error", """
                    {"timestamp":"2026-09-23T20:00:00Z","status":500,"error":"INTERNAL_SERVER_ERROR",\
                    "message":"An unexpected error occurred","path":"/api/v1/orders","correlationId":"abc-123"}"""}
    );

    private OpenApiSupport() {
    }

    public static OpenAPI openApi(String title, String description, String version) {
        return new OpenAPI()
                .info(new Info().title(title).description(description).version(version)
                        .license(new License().name("MIT").url("https://opensource.org/licenses/MIT")))
                .servers(List.of(new Server().url("/").description("API Gateway")))
                .components(new Components().addSecuritySchemes(BEARER_SCHEME, new SecurityScheme()
                        .type(SecurityScheme.Type.HTTP).scheme("bearer").bearerFormat("JWT")
                        .description("Token de POST /api/v1/auth/login (sin el prefijo Bearer)")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_SCHEME));
    }

    public static OperationCustomizer commonErrorResponses() {
        return (operation, handlerMethod) -> {
            ApiResponses responses = operation.getResponses();
            COMMON_ERRORS.forEach((code, description) -> responses.putIfAbsent(code, errorResponse(description)));
            return operation;
        };
    }

    private static ApiResponse errorResponse(String[] description) {
        return new ApiResponse()
                .description(description[0])
                .content(new Content().addMediaType(JSON,
                        new MediaType().addExamples("example", new Example().value(description[1]))));
    }
}
