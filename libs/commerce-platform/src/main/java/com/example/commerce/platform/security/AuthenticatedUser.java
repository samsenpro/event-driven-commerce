package com.example.commerce.platform.security;

/**
 * Identidad extraída del JWT. Los servicios de negocio confían en el token firmado y no
 * consultan la base de datos de usuarios (que es propiedad exclusiva de auth-service).
 */
public record AuthenticatedUser(Long id, String email, Role role) {

    public boolean isAdmin() {
        return role == Role.ADMIN;
    }

    @Override
    public String toString() {
        return "AuthenticatedUser{id=" + id + ", role=" + role + "}";
    }
}
