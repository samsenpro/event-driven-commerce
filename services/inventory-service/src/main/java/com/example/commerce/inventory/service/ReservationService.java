package com.example.commerce.inventory.service;

import com.example.commerce.events.EventEnvelope;
import com.example.commerce.events.payload.InventoryReleased;
import com.example.commerce.events.payload.InventoryReservationFailed;
import com.example.commerce.events.payload.InventoryReserved;
import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderConfirmed;
import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.events.payload.OrderLine;
import com.example.commerce.events.payload.StockLine;
import com.example.commerce.inventory.entity.Product;
import com.example.commerce.inventory.entity.Reservation;
import com.example.commerce.inventory.entity.ReservationLine;
import com.example.commerce.inventory.entity.ReservationStatus;
import com.example.commerce.inventory.repository.ProductRepository;
import com.example.commerce.inventory.repository.ReservationRepository;
import com.example.commerce.inventory.repository.StockItemRepository;
import com.example.commerce.platform.messaging.NonRetryableEventException;
import com.example.commerce.platform.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Participación del inventory-service en la saga. Cada método se ejecuta en la transacción del
 * {@code IdempotentEventProcessor} (evento procesado + cambios de stock + evento de salida).
 */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final ReservationRepository reservationRepository;
    private final StockItemRepository stockRepository;
    private final ProductRepository productRepository;
    private final OutboxWriter outbox;
    private final Clock clock;

    public ReservationService(ReservationRepository reservationRepository, StockItemRepository stockRepository,
                              ProductRepository productRepository, OutboxWriter outbox, Clock clock) {
        this.reservationRepository = reservationRepository;
        this.stockRepository = stockRepository;
        this.productRepository = productRepository;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Reserva <b>todas</b> las líneas o ninguna. Las líneas se reservan en orden de productId (orden de
     * bloqueo global, sin deadlocks entre pedidos concurrentes). Si alguna falla, las ya reservadas se
     * devuelven en la misma transacción y se publica INVENTORY_RESERVATION_FAILED.
     */
    public void reserve(EventEnvelope<OrderCreated> event) {
        OrderCreated order = event.payload();
        Optional<Reservation> existing = reservationRepository.findByOrderId(order.orderId());
        if (existing.isPresent()) {
            log.info("Reservation already exists for orderId={} status={}; ignoring ORDER_CREATED",
                    order.orderId(), existing.get().getStatus());
            return;
        }

        List<OrderLine> lines = order.items().stream().sorted(Comparator.comparing(OrderLine::productId)).toList();
        List<Long> notSellable = findNotSellable(lines);
        if (!notSellable.isEmpty()) {
            reject(order, "Unknown or inactive products", notSellable);
            return;
        }

        List<OrderLine> reserved = new ArrayList<>();
        List<Long> unavailable = new ArrayList<>();
        for (OrderLine line : lines) {
            if (stockRepository.reserve(line.productId(), line.quantity()) == 1) {
                reserved.add(line);
            } else {
                unavailable.add(line.productId());
            }
        }
        if (!unavailable.isEmpty()) {
            reserved.forEach(line -> stockRepository.release(line.productId(), line.quantity()));
            reject(order, "Insufficient stock", unavailable);
            return;
        }

        reservationRepository.save(Reservation.reserved(order.orderId(), order.userId(), toReservationLines(lines),
                clock.instant()));
        outbox.publish(new InventoryReserved(order.orderId(), order.userId(), toStockLines(lines)), order.orderId());
        log.info("Stock reserved orderId={} lines={}", order.orderId(), lines.size());
    }

    /** Pedido confirmado: el stock reservado se descuenta definitivamente. */
    public void commit(EventEnvelope<OrderConfirmed> event) {
        Long orderId = event.payload().orderId();
        Reservation reservation = reservationRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NonRetryableEventException("No reservation for confirmed order " + orderId));
        if (reservation.getStatus() != ReservationStatus.RESERVED) {
            log.info("Reservation of orderId={} is {}; ORDER_CONFIRMED ignored", orderId, reservation.getStatus());
            return;
        }
        reservation.getLines().forEach(line -> requireUpdated(
                stockRepository.commit(line.getProductId(), line.getQuantity()), line.getProductId()));
        reservation.commit(clock.instant());
        log.info("Stock committed orderId={}", orderId);
    }

    /**
     * Compensación: el pedido se canceló (pago rechazado, sin stock o por el cliente). Si había stock
     * reservado vuelve a estar disponible; si ya se había confirmado, se repone.
     * Si la cancelación llega antes que la creación, se deja un marcador VOIDED.
     */
    public void release(EventEnvelope<OrderCancelled> event) {
        OrderCancelled cancelled = event.payload();
        Optional<Reservation> found = reservationRepository.findByOrderId(cancelled.orderId());
        if (found.isEmpty()) {
            reservationRepository.save(Reservation.voided(cancelled.orderId(), cancelled.userId(), clock.instant()));
            log.info("ORDER_CANCELLED arrived before ORDER_CREATED; reservation voided orderId={}", cancelled.orderId());
            return;
        }
        Reservation reservation = found.get();
        switch (reservation.getStatus()) {
            case RESERVED -> reservation.getLines().forEach(line -> requireUpdated(
                    stockRepository.release(line.getProductId(), line.getQuantity()), line.getProductId()));
            case COMMITTED -> reservation.getLines().forEach(line ->
                    stockRepository.addAvailable(line.getProductId(), line.getQuantity()));
            default -> {
                log.info("Nothing to release for orderId={} (reservation {})", cancelled.orderId(), reservation.getStatus());
                return;
            }
        }
        reservation.release(clock.instant());
        outbox.publish(new InventoryReleased(cancelled.orderId(), cancelled.userId()), cancelled.orderId());
        log.info("Stock released orderId={} reason={}", cancelled.orderId(), cancelled.reason());
    }

    private List<Long> findNotSellable(List<OrderLine> lines) {
        Map<Long, Product> products = productRepository.findAllById(lines.stream().map(OrderLine::productId).toList())
                .stream().collect(Collectors.toMap(Product::getId, Function.identity()));
        return lines.stream()
                .map(OrderLine::productId)
                .filter(id -> !products.containsKey(id) || !products.get(id).isActive())
                .toList();
    }

    private void reject(OrderCreated order, String reason, List<Long> productIds) {
        reservationRepository.save(Reservation.rejected(order.orderId(), order.userId(), reason, clock.instant()));
        outbox.publish(new InventoryReservationFailed(order.orderId(), order.userId(), reason, productIds),
                order.orderId());
        log.info("Stock reservation failed orderId={} reason={} products={}", order.orderId(), reason, productIds);
    }

    /** Si una reserva confirmada no está en el stock, los datos son incoherentes: se aborta la transacción. */
    private static void requireUpdated(int updatedRows, Long productId) {
        if (updatedRows != 1) {
            throw new IllegalStateException("Reserved stock inconsistent for product " + productId);
        }
    }

    private static List<ReservationLine> toReservationLines(List<OrderLine> lines) {
        return lines.stream().map(line -> new ReservationLine(line.productId(), line.quantity())).toList();
    }

    private static List<StockLine> toStockLines(List<OrderLine> lines) {
        return lines.stream().map(line -> new StockLine(line.productId(), line.quantity())).toList();
    }
}
