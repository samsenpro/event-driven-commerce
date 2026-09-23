package com.example.commerce.inventory.controller;

import com.example.commerce.inventory.dto.ProductDtos.StockAdjustmentRequest;
import com.example.commerce.inventory.dto.ProductDtos.StockResponse;
import com.example.commerce.inventory.service.StockService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/inventory")
@Tag(name = "Inventory")
public class InventoryController {

    private final StockService stockService;

    public InventoryController(StockService stockService) {
        this.stockService = stockService;
    }

    @GetMapping("/{productId}")
    @Operation(summary = "Consultar stock disponible y reservado (ADMIN)")
    public StockResponse find(@PathVariable Long productId) {
        return stockService.find(productId);
    }

    @PostMapping("/{productId}/add")
    @Operation(summary = "Reponer stock (ADMIN)")
    public StockResponse add(@PathVariable Long productId, @Valid @RequestBody StockAdjustmentRequest request) {
        return stockService.add(productId, request.quantity());
    }
}
