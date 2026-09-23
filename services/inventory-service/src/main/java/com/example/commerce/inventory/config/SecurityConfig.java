package com.example.commerce.inventory.config;

import com.example.commerce.platform.security.CommerceSecurity;
import com.example.commerce.platform.security.Role;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Consultar el catálogo: cualquier usuario autenticado. Gestionar productos y ver o modificar el
 * stock: solo ADMIN.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, CommerceSecurity commerceSecurity)
            throws Exception {
        return commerceSecurity.build(http, rules -> rules
                .requestMatchers(HttpMethod.GET, "/api/v1/products", "/api/v1/products/**").authenticated()
                .requestMatchers("/api/v1/products", "/api/v1/products/**").hasRole(Role.ADMIN.name())
                .requestMatchers("/api/v1/inventory/**").hasRole(Role.ADMIN.name()));
    }
}
