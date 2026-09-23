package com.example.commerce.notification.controller;

import com.example.commerce.notification.dto.NotificationResponse;
import com.example.commerce.notification.service.NotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/notifications")
@Tag(name = "Notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping
    @Operation(summary = "Notificaciones enviadas para un pedido, en orden (ADMIN)")
    public List<NotificationResponse> findByOrder(@RequestParam Long orderId) {
        return notificationService.findByOrderId(orderId);
    }
}
