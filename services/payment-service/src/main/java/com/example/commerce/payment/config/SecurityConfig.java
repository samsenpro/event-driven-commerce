package com.example.commerce.payment.config;

import com.example.commerce.platform.security.CommerceSecurity;
import com.example.commerce.platform.security.Role;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, CommerceSecurity commerceSecurity)
            throws Exception {
        return commerceSecurity.build(http, rules -> rules
                .requestMatchers("/api/v1/payments/**").hasRole(Role.ADMIN.name()));
    }
}
