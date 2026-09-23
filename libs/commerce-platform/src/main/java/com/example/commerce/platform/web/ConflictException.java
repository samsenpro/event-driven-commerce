package com.example.commerce.platform.web;

import org.springframework.http.HttpStatus;

/**
 * Conflicto con el estado actual del recurso (HTTP 409).
 */
public class ConflictException extends ApiException {

    public ConflictException(String message) {
        super(HttpStatus.CONFLICT, message);
    }
}
