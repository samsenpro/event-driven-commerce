package com.example.commerce.inventory.service;

import com.example.commerce.events.payload.ProductChanged;
import com.example.commerce.inventory.dto.ProductDtos.CreateProductRequest;
import com.example.commerce.inventory.dto.ProductDtos.ProductResponse;
import com.example.commerce.inventory.dto.ProductDtos.UpdateProductRequest;
import com.example.commerce.inventory.entity.Product;
import com.example.commerce.inventory.entity.StockItem;
import com.example.commerce.inventory.repository.ProductRepository;
import com.example.commerce.inventory.repository.StockItemRepository;
import com.example.commerce.platform.outbox.OutboxWriter;
import com.example.commerce.platform.web.ConflictException;
import com.example.commerce.platform.web.NotFoundException;
import com.example.commerce.platform.web.PageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Locale;

/**
 * Gestión del catálogo. Cada alta o cambio publica PRODUCT_CHANGED (vía outbox) con el estado
 * completo del producto, para que otros servicios mantengan su propia réplica.
 */
@Service
public class ProductService {

    private static final Logger log = LoggerFactory.getLogger(ProductService.class);

    private final ProductRepository productRepository;
    private final StockItemRepository stockRepository;
    private final OutboxWriter outbox;
    private final Clock clock;

    public ProductService(ProductRepository productRepository, StockItemRepository stockRepository,
                          OutboxWriter outbox, Clock clock) {
        this.productRepository = productRepository;
        this.stockRepository = stockRepository;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public ProductResponse create(CreateProductRequest request) {
        String sku = request.sku().trim().toUpperCase(Locale.ROOT);
        if (productRepository.existsBySku(sku)) {
            throw new ConflictException("SKU already exists: " + sku);
        }
        Product product = productRepository.saveAndFlush(
                new Product(sku, request.name().trim(), request.description(), request.price(), clock.instant()));
        stockRepository.save(new StockItem(product.getId(), request.initialStock(), clock.instant()));
        publishChange(product);
        log.info("Product created productId={} sku={} stock={}", product.getId(), sku, request.initialStock());
        return ProductResponse.from(product);
    }

    @Transactional
    public ProductResponse update(Long id, UpdateProductRequest request) {
        Product product = find(id);
        product.update(request.name().trim(), request.description(), request.price(), request.active(),
                clock.instant());
        productRepository.saveAndFlush(product);
        publishChange(product);
        log.info("Product updated productId={} price={} active={}", id, product.getPrice(), product.isActive());
        return ProductResponse.from(product);
    }

    @Transactional(readOnly = true)
    public ProductResponse findById(Long id) {
        return ProductResponse.from(find(id));
    }

    @Transactional(readOnly = true)
    public PageResponse<ProductResponse> findPage(int page, int size) {
        return PageResponse.from(productRepository.findAll(PageRequest.of(page, size, Sort.by("id"))),
                ProductResponse::from);
    }

    private void publishChange(Product product) {
        outbox.publish(new ProductChanged(product.getId(), product.getSku(), product.getName(), product.getPrice(),
                product.isActive(), product.getUpdatedAt()), product.getId());
    }

    private Product find(Long id) {
        return productRepository.findById(id).orElseThrow(() -> new NotFoundException("Product not found: " + id));
    }
}
