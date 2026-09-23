package com.example.commerce.platform.web;

import org.springframework.http.HttpStatus;

/**
 * El recurso no existe o no pertenece al usuario (HTTP 404).
 */
public class NotFoundException extends ApiException {

    public NotFoundException(String message) {
        super(HttpStatus.NOT_FOUND, message);
    }
}
