package com.example.commerce.platform.web;

import org.springframework.http.HttpStatus;

/**
 * Base de las excepciones de negocio expuestas por REST. Cada subclase declara su código HTTP.
 */
public abstract class ApiException extends RuntimeException {

    private final HttpStatus status;

    protected ApiException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
