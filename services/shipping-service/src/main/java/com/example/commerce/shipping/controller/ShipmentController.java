package com.example.commerce.shipping.controller;

import com.example.commerce.shipping.dto.ShipmentResponse;
import com.example.commerce.shipping.service.ShippingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/shipments")
@Tag(name = "Shipments")
public class ShipmentController {

    private final ShippingService shippingService;

    public ShipmentController(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @GetMapping("/{orderId}")
    @Operation(summary = "Consultar el envío de un pedido (ADMIN)")
    public ShipmentResponse findByOrderId(@PathVariable Long orderId) {
        return shippingService.findByOrderId(orderId);
    }
}
