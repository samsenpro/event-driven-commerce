package com.example.commerce.inventory.repository;

import com.example.commerce.inventory.entity.StockItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Escrituras de stock como UPDATE condicionales: la comprobación y el cambio ocurren en una
 * sentencia con bloqueo de fila, así que dos reservas simultáneas nunca dejan stock negativo.
 * Devuelven las filas afectadas: 0 significa que la condición no se cumplía.
 */
public interface StockItemRepository extends JpaRepository<StockItem, Long> {

    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE stock_items SET available = available - :quantity, reserved = reserved + :quantity,
                   updated_at = now()
             WHERE product_id = :productId AND available >= :quantity
            """, nativeQuery = true)
    int reserve(@Param("productId") Long productId, @Param("quantity") int quantity);

    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE stock_items SET available = available + :quantity, reserved = reserved - :quantity,
                   updated_at = now()
             WHERE product_id = :productId AND reserved >= :quantity
            """, nativeQuery = true)
    int release(@Param("productId") Long productId, @Param("quantity") int quantity);

    /** Pedido confirmado: las unidades reservadas salen definitivamente del stock. */
    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE stock_items SET reserved = reserved - :quantity, updated_at = now()
             WHERE product_id = :productId AND reserved >= :quantity
            """, nativeQuery = true)
    int commit(@Param("productId") Long productId, @Param("quantity") int quantity);

    @Modifying(flushAutomatically = true)
    @Query(value = """
            UPDATE stock_items SET available = available + :quantity, updated_at = now()
             WHERE product_id = :productId
            """, nativeQuery = true)
    int addAvailable(@Param("productId") Long productId, @Param("quantity") int quantity);
}
