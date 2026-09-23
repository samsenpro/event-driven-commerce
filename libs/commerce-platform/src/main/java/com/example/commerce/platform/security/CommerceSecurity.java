package com.example.commerce.platform.security;

import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.AuthorizeHttpRequestsConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Configuración de seguridad común: stateless, JWT, errores en JSON y endpoints públicos de
 * health y documentación. Cada servicio solo declara sus propias reglas de autorización.
 */
public class CommerceSecurity {

    private static final String[] PUBLIC_PATHS = {
            "/actuator/health", "/actuator/health/**",
            "/swagger-ui.html", "/swagger-ui/**", "/v3/api-docs/**",
            "/error"
    };

    private final JwtAuthenticationFilter jwtAuthenticationFilter;
    private final JsonSecurityErrorHandler errorHandler;

    public CommerceSecurity(JwtAuthenticationFilter jwtAuthenticationFilter, JsonSecurityErrorHandler errorHandler) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
        this.errorHandler = errorHandler;
    }

    public SecurityFilterChain build(
            HttpSecurity http,
            Customizer<AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry> rules)
            throws Exception {
        return http
                // CSRF deshabilitado de forma deliberada: API stateless autenticada solo con el header
                // Authorization: Bearer. Sin cookies de sesión no hay credenciales que un ataque CSRF
                // pueda hacer enviar al navegador.
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers(PUBLIC_PATHS).permitAll();
                    auth.requestMatchers("/actuator/**").hasRole(Role.ADMIN.name());
                    rules.customize(auth);
                    auth.anyRequest().authenticated();
                })
                .exceptionHandling(handling -> handling
                        .authenticationEntryPoint(errorHandler)
                        .accessDeniedHandler(errorHandler))
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class)
                .build();
    }
}
