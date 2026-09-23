package com.example.commerce.order.controller;

import com.example.commerce.order.dto.CreateOrderRequest;
import com.example.commerce.order.dto.OrderResponse;
import com.example.commerce.order.dto.PageResponse;
import com.example.commerce.order.service.OrderService;
import com.example.commerce.platform.security.AuthenticatedUser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/api/v1/orders")
@Tag(name = "Orders")
public class OrderController {

    private static final int MAX_PAGE_SIZE = 100;

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * 202 Accepted: el pedido queda registrado como PENDING y el resultado (confirmado o cancelado)
     * se decide de forma asíncrona en la saga.
     */
    @PostMapping
    @Operation(summary = "Crear un pedido (asíncrono: queda PENDING y la saga decide el resultado)")
    public ResponseEntity<OrderResponse> create(@AuthenticationPrincipal AuthenticatedUser user,
                                                @Valid @RequestBody CreateOrderRequest request) {
        OrderResponse order = orderService.create(user, request);
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/orders/" + order.id()))
                .body(order);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Consultar un pedido propio (o cualquiera si es ADMIN)")
    public OrderResponse findById(@AuthenticationPrincipal AuthenticatedUser user, @PathVariable Long id) {
        return orderService.findById(user, id);
    }

    @GetMapping
    @Operation(summary = "Listar pedidos: los propios (USER) o todos (ADMIN)")
    public PageResponse<OrderResponse> findPage(@AuthenticationPrincipal AuthenticatedUser user,
                                                @RequestParam(defaultValue = "0") @Min(0) int page,
                                                @RequestParam(defaultValue = "20") @Min(1) @Max(MAX_PAGE_SIZE) int size) {
        return orderService.findPage(user, page, size);
    }

    @PatchMapping("/{id}/cancel")
    @Operation(summary = "Cancelar un pedido propio antes del envío (publica ORDER_CANCELLED)")
    public OrderResponse cancel(@AuthenticationPrincipal AuthenticatedUser user, @PathVariable Long id) {
        return orderService.cancel(user, id);
    }
}
