package com.example.commerce.order.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.ProductChanged;
import com.example.commerce.order.entity.CatalogProduct;
import com.example.commerce.order.repository.CatalogProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Mantiene la réplica local del catálogo a partir de {@code products.changed}. Los eventos de un
 * mismo producto llegan en orden (clave = productId); aun así se descartan los que sean más
 * antiguos que el estado ya aplicado, lo que hace la operación segura ante reenvíos.
 */
@Service
public class CatalogReplicaService {

    private static final Logger log = LoggerFactory.getLogger(CatalogReplicaService.class);

    private final CatalogProductRepository repository;

    public CatalogReplicaService(CatalogProductRepository repository) {
        this.repository = repository;
    }

    public void apply(EventEnvelope<ProductChanged> event) {
        ProductChanged product = event.payload();
        repository.findById(product.productId()).ifPresentOrElse(
                existing -> {
                    if (existing.isNewerThan(product.updatedAt())) {
                        log.info("Stale product event ignored productId={}", product.productId());
                        return;
                    }
                    existing.apply(product.sku(), product.name(), product.price(), product.active(), product.updatedAt());
                },
                () -> repository.save(new CatalogProduct(product.productId(), product.sku(), product.name(),
                        product.price(), product.active(), product.updatedAt())));
        log.info("Catalog replica updated productId={} price={} active={}",
                product.productId(), product.price(), product.active());
    }
}
