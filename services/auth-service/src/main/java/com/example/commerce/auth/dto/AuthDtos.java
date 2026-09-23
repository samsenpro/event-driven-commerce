package com.example.commerce.auth.dto;

import com.example.commerce.auth.entity.User;
import com.example.commerce.platform.security.Role;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public final class AuthDtos {

    /** Mínimo 8 caracteres con al menos una mayúscula, una minúscula y un número. */
    public static final String PASSWORD_REGEX = "^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d).{8,}$";
    public static final String PASSWORD_MESSAGE =
            "must be at least 8 characters long and contain an uppercase letter, a lowercase letter and a number";
    /** BCrypt solo tiene en cuenta los primeros 72 bytes. */
    public static final int PASSWORD_MAX_LENGTH = 72;

    private AuthDtos() {
    }

    public record RegisterRequest(
            @Schema(example = "Jane Doe") @NotBlank @Size(max = 100) String name,
            @Schema(example = "user@example.com") @NotBlank @Email @Size(max = 255) String email,
            @Schema(example = "Password123!") @NotBlank @Size(max = PASSWORD_MAX_LENGTH)
            @Pattern(regexp = PASSWORD_REGEX, message = PASSWORD_MESSAGE) String password
    ) {

        @Override
        public String toString() {
            return "RegisterRequest{email=" + email + "}";
        }
    }

    public record LoginRequest(
            @Schema(example = "user@example.com") @NotBlank @Size(max = 255) String email,
            @Schema(example = "Password123!") @NotBlank @Size(max = 128) String password
    ) {

        @Override
        public String toString() {
            return "LoginRequest{email=" + email + "}";
        }
    }

    public record AuthResponse(String accessToken, String tokenType, long expiresIn) {

        public static AuthResponse bearer(String token, long expiresInSeconds) {
            return new AuthResponse(token, "Bearer", expiresInSeconds);
        }

        @Override
        public String toString() {
            return "AuthResponse{tokenType=" + tokenType + ", expiresIn=" + expiresIn + "}";
        }
    }

    public record UserResponse(Long id, String name, String email, Role role, Instant createdAt) {

        public static UserResponse from(User user) {
            return new UserResponse(user.getId(), user.getName(), user.getEmail(), user.getRole(), user.getCreatedAt());
        }
    }
}
