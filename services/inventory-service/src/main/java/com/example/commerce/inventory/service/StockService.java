package com.example.commerce.inventory.service;

import com.example.commerce.inventory.dto.ProductDtos.StockResponse;
import com.example.commerce.inventory.entity.StockItem;
import com.example.commerce.inventory.repository.StockItemRepository;
import com.example.commerce.platform.web.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consulta y reposición manual de stock (ADMIN). Las reservas no pasan por aquí: llegan por eventos.
 */
@Service
public class StockService {

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private final StockItemRepository stockRepository;

    public StockService(StockItemRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Transactional(readOnly = true)
    public StockResponse find(Long productId) {
        StockItem stock = stockRepository.findById(productId)
                .orElseThrow(() -> new NotFoundException("Product not found: " + productId));
        return new StockResponse(stock.getProductId(), stock.getAvailable(), stock.getReserved(), stock.getUpdatedAt());
    }

    @Transactional
    public StockResponse add(Long productId, int quantity) {
        if (stockRepository.addAvailable(productId, quantity) == 0) {
            throw new NotFoundException("Product not found: " + productId);
        }
        log.info("Stock added productId={} quantity={}", productId, quantity);
        return find(productId);
    }
}
