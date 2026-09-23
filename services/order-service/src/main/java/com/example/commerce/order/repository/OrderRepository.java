package com.example.commerce.order.repository;

import com.example.commerce.order.entity.Order;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {

    Optional<Order> findByIdAndUserId(Long id, Long userId);

    Page<Order> findAllByUserId(Long userId, Pageable pageable);
}
