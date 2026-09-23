package com.example.commerce.order.service;

import com.example.commerce.events.payload.OrderCancelled;
import com.example.commerce.events.payload.OrderCreated;
import com.example.commerce.events.payload.OrderLine;
import com.example.commerce.order.dto.CreateOrderRequest;
import com.example.commerce.order.dto.OrderResponse;
import com.example.commerce.platform.web.PageResponse;
import com.example.commerce.order.entity.CancellationReason;
import com.example.commerce.order.entity.CatalogProduct;
import com.example.commerce.order.entity.Order;
import com.example.commerce.order.entity.OrderItem;
import com.example.commerce.order.entity.OrderStatus;
import com.example.commerce.order.repository.CatalogProductRepository;
import com.example.commerce.order.repository.OrderRepository;
import com.example.commerce.platform.outbox.OutboxWriter;
import com.example.commerce.platform.security.AuthenticatedUser;
import com.example.commerce.platform.web.BusinessRuleException;
import com.example.commerce.platform.web.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Operaciones REST sobre pedidos. Cada cambio de estado y su evento se guardan en la
 * <b>misma transacción</b> (Order + OutboxEvent): o se confirman ambos o ninguno.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final Sort NEWEST_FIRST = Sort.by(Sort.Direction.DESC, "createdAt", "id");

    private final OrderRepository orderRepository;
    private final CatalogProductRepository catalogRepository;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderService(OrderRepository orderRepository, CatalogProductRepository catalogRepository,
                        OutboxWriter outbox, Clock clock) {
        this.orderRepository = orderRepository;
        this.catalogRepository = catalogRepository;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public OrderResponse create(AuthenticatedUser user, CreateOrderRequest request) {
        SortedMap<Long, Integer> quantities = consolidate(request);
        Map<Long, CatalogProduct> catalog = loadSellableProducts(quantities.keySet());

        List<OrderItem> items = quantities.entrySet().stream()
                .map(line -> {
                    CatalogProduct product = catalog.get(line.getKey());
                    return new OrderItem(product.getProductId(), product.getName(), line.getValue(), product.getPrice());
                })
                .toList();

        Order order = orderRepository.saveAndFlush(Order.place(user.id(), items, clock.instant()));
        outbox.publish(new OrderCreated(order.getId(), order.getUserId(), toLines(order), order.getTotalAmount()),
                order.getId());

        log.info("Order created orderId={} userId={} total={} items={}",
                order.getId(), order.getUserId(), order.getTotalAmount(), items.size());
        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse findById(AuthenticatedUser user, Long orderId) {
        return OrderResponse.from(findAccessibleOrder(user, orderId));
    }

    /** USER: sus pedidos. ADMIN: todos. */
    @Transactional(readOnly = true)
    public PageResponse<OrderResponse> findPage(AuthenticatedUser user, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, NEWEST_FIRST);
        return PageResponse.from(user.isAdmin()
                ? orderRepository.findAll(pageable)
                : orderRepository.findAllByUserId(user.id(), pageable), OrderResponse::from);
    }

    /**
     * Cancelación del cliente. Publica ORDER_CANCELLED y los demás servicios compensan por su cuenta:
     * inventory libera el stock y payment reembolsa si ya había cobrado.
     */
    @Transactional
    public OrderResponse cancel(AuthenticatedUser user, Long orderId) {
        Order order = findAccessibleOrder(user, orderId);
        if (!order.canMoveTo(OrderStatus.CANCELLED)) {
            throw new BusinessRuleException("Order " + orderId + " cannot be cancelled in status " + order.getStatus());
        }
        order.cancel(CancellationReason.CUSTOMER_REQUEST, clock.instant());
        outbox.publish(new OrderCancelled(order.getId(), order.getUserId(), CancellationReason.CUSTOMER_REQUEST.name()),
                order.getId());
        log.info("Order cancelled by customer orderId={} userId={}", order.getId(), order.getUserId());
        return OrderResponse.from(orderRepository.saveAndFlush(order));
    }

    static List<OrderLine> toLines(Order order) {
        return order.getItems().stream()
                .map(item -> new OrderLine(item.getProductId(), item.getProductName(), item.getQuantity(),
                        item.getUnitPrice()))
                .toList();
    }

    /** Un pedido ajeno se comporta como inexistente (404) para no revelar qué ids existen. */
    private Order findAccessibleOrder(AuthenticatedUser user, Long orderId) {
        return (user.isAdmin() ? orderRepository.findById(orderId) : orderRepository.findByIdAndUserId(orderId, user.id()))
                .orElseThrow(() -> new NotFoundException("Order not found: " + orderId));
    }

    /** Suma las líneas repetidas del mismo producto y las ordena por productId. */
    private static SortedMap<Long, Integer> consolidate(CreateOrderRequest request) {
        SortedMap<Long, Integer> quantities = new TreeMap<>();
        request.items().forEach(item -> quantities.merge(item.productId(), item.quantity(), Integer::sum));
        quantities.forEach((productId, quantity) -> {
            if (quantity > CreateOrderRequest.MAX_QUANTITY) {
                throw new BusinessRuleException("Quantity for product " + productId + " exceeds "
                        + CreateOrderRequest.MAX_QUANTITY);
            }
        });
        return quantities;
    }

    private Map<Long, CatalogProduct> loadSellableProducts(Set<Long> productIds) {
        Map<Long, CatalogProduct> catalog = catalogRepository.findAllById(productIds).stream()
                .collect(Collectors.toMap(CatalogProduct::getProductId, Function.identity()));
        for (Long productId : productIds) {
            CatalogProduct product = catalog.get(productId);
            if (product == null) {
                throw new BusinessRuleException("Unknown product: " + productId);
            }
            if (!product.isActive()) {
                throw new BusinessRuleException("Product is not available: " + productId);
            }
        }
        return catalog;
    }
}
