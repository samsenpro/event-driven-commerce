package com.example.commerce.order.repository;

import com.example.commerce.order.entity.CatalogProduct;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CatalogProductRepository extends JpaRepository<CatalogProduct, Long> {
}
