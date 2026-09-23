package com.example.commerce.notification.repository;

import com.example.commerce.notification.entity.Notification;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    List<Notification> findAllByOrderIdOrderBySentAtAsc(Long orderId);
}
