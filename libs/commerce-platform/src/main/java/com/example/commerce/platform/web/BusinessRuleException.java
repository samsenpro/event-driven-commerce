package com.example.commerce.platform.web;

import org.springframework.http.HttpStatus;

/**
 * Petición bien formada que viola una regla de negocio (HTTP 422).
 */
public class BusinessRuleException extends ApiException {

    public BusinessRuleException(String message) {
        super(HttpStatus.UNPROCESSABLE_ENTITY, message);
    }
}
