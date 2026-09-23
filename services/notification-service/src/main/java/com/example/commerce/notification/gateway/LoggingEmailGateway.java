package com.example.commerce.notification.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Simula el envío registrando el email en el log. Una implementación real lanzaría una excepción
 * ante fallos transitorios del proveedor para que Kafka reintentara el evento.
 */
@Component
public class LoggingEmailGateway implements EmailGateway {

    private static final Logger log = LoggerFactory.getLogger(LoggingEmailGateway.class);

    @Override
    public void send(String recipient, String subject, String body) {
        log.info("[simulated email] to={} subject=\"{}\" body=\"{}\"", recipient, subject, body);
    }
}
