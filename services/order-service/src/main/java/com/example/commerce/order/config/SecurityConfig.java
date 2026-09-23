package com.example.commerce.order.config;

import com.example.commerce.platform.security.CommerceSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Todos los endpoints de pedidos requieren un usuario autenticado. La propiedad del pedido
 * (un USER solo ve y cancela los suyos) se comprueba en {@code OrderService}.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, CommerceSecurity commerceSecurity)
            throws Exception {
        return commerceSecurity.build(http, rules -> {
            // Sin reglas adicionales: /api/v1/orders/** exige autenticación (regla por defecto)
        });
    }
}
