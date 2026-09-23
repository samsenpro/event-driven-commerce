package com.example.commerce.platform.web;

import org.springframework.http.HttpStatus;

/**
 * Parámetros de la petición incorrectos (HTTP 400).
 */
public class BadRequestException extends ApiException {

    public BadRequestException(String message) {
        super(HttpStatus.BAD_REQUEST, message);
    }
}
