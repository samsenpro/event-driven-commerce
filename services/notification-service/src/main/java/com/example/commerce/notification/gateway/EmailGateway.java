package com.example.commerce.notification.gateway;

/**
 * Integración con el proveedor de email (SES, SendGrid...). Aquí solo se simula.
 */
public interface EmailGateway {

    void send(String recipient, String subject, String body);
}
