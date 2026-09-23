package com.example.commerce.platform.web;

import org.springframework.http.HttpStatus;

/**
 * Credenciales incorrectas (HTTP 401).
 */
public class UnauthorizedException extends ApiException {

    public UnauthorizedException(String message) {
        super(HttpStatus.UNAUTHORIZED, message);
    }
}
