package com.example.commerce.auth.service;

import com.example.commerce.auth.dto.AuthDtos.AuthResponse;
import com.example.commerce.auth.dto.AuthDtos.LoginRequest;
import com.example.commerce.auth.dto.AuthDtos.RegisterRequest;
import com.example.commerce.auth.dto.AuthDtos.UserResponse;
import com.example.commerce.auth.entity.User;
import com.example.commerce.auth.repository.UserRepository;
import com.example.commerce.platform.security.AuthenticatedUser;
import com.example.commerce.platform.security.JwtTokenService;
import com.example.commerce.platform.security.Role;
import com.example.commerce.platform.web.ConflictException;
import com.example.commerce.platform.web.NotFoundException;
import com.example.commerce.platform.web.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Locale;
import java.util.Optional;

@Service
public class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);
    private static final String INVALID_CREDENTIALS = "Invalid email or password";

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenService tokenService;
    private final Clock clock;
    private final String dummyHash;

    public AuthService(UserRepository userRepository, PasswordEncoder passwordEncoder, JwtTokenService tokenService,
                       Clock clock) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.tokenService = tokenService;
        this.clock = clock;
        this.dummyHash = passwordEncoder.encode("timing-equalization-placeholder");
    }

    @Transactional
    public UserResponse register(RegisterRequest request) {
        String email = normalize(request.email());
        if (userRepository.existsByEmail(email)) {
            throw new ConflictException("Email is already registered");
        }
        try {
            User user = userRepository.saveAndFlush(new User(request.name().trim(), email,
                    passwordEncoder.encode(request.password()), Role.USER, clock.instant()));
            log.info("User registered userId={}", user.getId());
            return UserResponse.from(user);
        } catch (DataIntegrityViolationException ex) {
            throw new ConflictException("Email is already registered");
        }
    }

    /**
     * Email inexistente, contraseña incorrecta y cuenta deshabilitada devuelven el mismo 401 para
     * no permitir la enumeración de usuarios.
     */
    @Transactional(readOnly = true)
    public AuthResponse login(LoginRequest request) {
        Optional<User> candidate = userRepository.findByEmail(normalize(request.email()));
        // BCrypt se ejecuta siempre (con un hash ficticio si el email no existe) para que el tiempo
        // de respuesta no delate qué emails están registrados.
        boolean passwordMatches = passwordEncoder.matches(request.password(),
                candidate.map(User::getPassword).orElse(dummyHash));
        if (candidate.isEmpty() || !passwordMatches || !candidate.get().isEnabled()) {
            log.info("Failed login attempt");
            throw new UnauthorizedException(INVALID_CREDENTIALS);
        }
        User user = candidate.get();
        log.info("User logged in userId={}", user.getId());
        String token = tokenService.issue(new AuthenticatedUser(user.getId(), user.getEmail(), user.getRole()));
        return AuthResponse.bearer(token, tokenService.expirationSeconds());
    }

    @Transactional(readOnly = true)
    public UserResponse me(AuthenticatedUser principal) {
        return userRepository.findById(principal.id())
                .map(UserResponse::from)
                .orElseThrow(() -> new NotFoundException("User not found"));
    }

    static String normalize(String email) {
        return email.trim().toLowerCase(Locale.ROOT);
    }
}
