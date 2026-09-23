package com.example.commerce.platform.security;

public enum Role {
    USER,
    ADMIN;

    public String authority() {
        return "ROLE_" + name();
    }
}
