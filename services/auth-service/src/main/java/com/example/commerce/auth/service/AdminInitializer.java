package com.example.commerce.auth.service;

import com.example.commerce.auth.dto.AuthDtos;
import com.example.commerce.auth.entity.User;
import com.example.commerce.auth.repository.UserRepository;
import com.example.commerce.platform.security.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.regex.Pattern;

/**
 * Crea el administrador inicial (ADMIN_EMAIL / ADMIN_PASSWORD) si se configuró y aún no existe.
 */
@Component
public class AdminInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AdminInitializer.class);
    private static final Pattern PASSWORD_POLICY = Pattern.compile(AuthDtos.PASSWORD_REGEX);

    private final AdminProperties properties;
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final Clock clock;

    public AdminInitializer(AdminProperties properties, UserRepository userRepository,
                            PasswordEncoder passwordEncoder, Clock clock) {
        this.properties = properties;
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.clock = clock;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        if (!properties.isConfigured()) {
            log.info("Admin seed skipped: ADMIN_EMAIL / ADMIN_PASSWORD not set");
            return;
        }
        if (!PASSWORD_POLICY.matcher(properties.password()).matches()) {
            log.warn("Admin seed skipped: ADMIN_PASSWORD does not satisfy the password policy");
            return;
        }
        String email = AuthService.normalize(properties.email());
        if (userRepository.existsByEmail(email)) {
            return;
        }
        User admin = userRepository.save(new User(properties.name(), email,
                passwordEncoder.encode(properties.password()), Role.ADMIN, clock.instant()));
        log.info("Initial admin account created userId={}", admin.getId());
    }

    @ConfigurationProperties(prefix = "commerce.admin")
    public record AdminProperties(String email, String password, String name) {

        public AdminProperties {
            name = name == null || name.isBlank() ? "Administrator" : name.trim();
        }

        boolean isConfigured() {
            return email != null && !email.isBlank() && password != null && !password.isBlank();
        }

        @Override
        public String toString() {
            return "AdminProperties{configured=" + isConfigured() + "}";
        }
    }
}
