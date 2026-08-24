package com.z4greed.inventory.repository;

import com.z4greed.inventory.entity.InventoryEntity;
import org.springframework.data.jpa.repository.*;

public interface InventoryRepository extends JpaRepository<InventoryEntity, Long> {
  @Modifying
  @Query(
      "update InventoryEntity inventoryEntity set inventoryEntity.availableQuantity=inventoryEntity.availableQuantity-:quantity,inventoryEntity.reservedQuantity=inventoryEntity.reservedQuantity+:quantity,inventoryEntity.updatedAt=CURRENT_TIMESTAMP where inventoryEntity.productId=:productId and inventoryEntity.availableQuantity>=:quantity")
  int reserve(Long productId, int quantity);

  @Modifying
  @Query(
      "update InventoryEntity inventoryEntity set inventoryEntity.availableQuantity=inventoryEntity.availableQuantity+:quantity,inventoryEntity.reservedQuantity=inventoryEntity.reservedQuantity-:quantity,inventoryEntity.updatedAt=CURRENT_TIMESTAMP where inventoryEntity.productId=:productId and inventoryEntity.reservedQuantity>=:quantity")
  int release(Long productId, int quantity);
}
