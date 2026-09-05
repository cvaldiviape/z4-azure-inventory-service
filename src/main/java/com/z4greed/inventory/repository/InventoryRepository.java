package com.z4greed.inventory.repository;

import com.z4greed.inventory.entity.InventoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface InventoryRepository extends JpaRepository<InventoryEntity, Long> {
  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(value = """
      UPDATE inventories
      SET available_quantity = available_quantity - :quantity,
          reserved_quantity = reserved_quantity + :quantity,
          version = version + 1,
          updated_at = CURRENT_TIMESTAMP
      WHERE product_id = :productId
        AND available_quantity >= :quantity
      """, nativeQuery = true)
  int reserveIfAvailable(@Param("productId") Long productId, @Param("quantity") Integer quantity);

  @Modifying(flushAutomatically = true, clearAutomatically = true)
  @Query(value = """
      UPDATE inventories
      SET available_quantity = available_quantity + :quantity,
          reserved_quantity = reserved_quantity - :quantity,
          version = version + 1,
          updated_at = CURRENT_TIMESTAMP
      WHERE product_id = :productId
        AND reserved_quantity >= :quantity
      """, nativeQuery = true)
  int releaseIfReserved(@Param("productId") Long productId, @Param("quantity") Integer quantity);
}
