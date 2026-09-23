package com.example.commerce.auth;

import com.example.commerce.platform.security.AuthenticatedUser;
import com.example.commerce.platform.security.JwtTokenService;
import com.example.commerce.platform.security.Role;
import com.example.commerce.testing.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AuthIntegrationTest extends AbstractIntegrationTest {

    private static final String PASSWORD = "Password123";

    @Autowired
    private JwtTokenService tokenService;

    @Test
    void registerLoginAndUseTheToken() throws Exception {
        String email = "user" + uniqueId() + "@test.local";
        register(email).andExpect(status().isCreated())
                .andExpect(jsonPath("$.role").value("USER"))
                .andExpect(jsonPath("$.password").doesNotExist());

        MvcResult login = mockMvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("email", email.toUpperCase(), "password", PASSWORD))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tokenType").value("Bearer"))
                .andExpect(jsonPath("$.expiresIn").value(3600))
                .andReturn();
        String token = objectMapper.readTree(login.getResponse().getContentAsString()).get("accessToken").asText();

        // El token es válido para cualquier servicio que comparta la clave
        AuthenticatedUser user = tokenService.parse(token).orElseThrow();
        assertThat(user.email()).isEqualTo(email);
        assertThat(user.role()).isEqualTo(Role.USER);

        mockMvc.perform(get("/api/v1/auth/me").header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.email").value(email));
    }

    @Test
    void duplicatedEmailAndInvalidDataAreRejected() throws Exception {
        String email = "dup" + uniqueId() + "@test.local";
        register(email).andExpect(status().isCreated());
        register(email).andExpect(status().isConflict());

        mockMvc.perform(post("/api/v1/auth/register").contentType(MediaType.APPLICATION_JSON)
                        .content(json(Map.of("name", "", "email", "nope", "password", "weak"))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[*].field").value(hasItems("name", "email", "password")));
    }

    @Test
    void loginErrorsAreGeneric() throws Exception {
        String email = "generic" + uniqueId() + "@test.local";
        register(email).andExpect(status().isCreated());

        for (Map<String, String> credentials : List.of(
                Map.of("email", email, "password", "WrongPassword1"),
                Map.of("email", "missing" + uniqueId() + "@test.local", "password", PASSWORD))) {
            mockMvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
                            .content(json(credentials)))
                    .andExpect(status().isUnauthorized())
                    .andExpect(jsonPath("$.message").value("Invalid email or password"));
        }
    }

    @Test
    void meRequiresAToken() throws Exception {
        mockMvc.perform(get("/api/v1/auth/me"))
                .andExpect(status().isUnauthorized());
    }

    private ResultActions register(String email) throws Exception {
        return mockMvc.perform(post("/api/v1/auth/register").contentType(MediaType.APPLICATION_JSON)
                .content(json(Map.of("name", "Jane", "email", email, "password", PASSWORD))));
    }
}
