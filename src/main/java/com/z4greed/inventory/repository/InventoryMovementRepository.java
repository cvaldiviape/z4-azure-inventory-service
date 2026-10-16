package com.z4greed.inventory.repository;

import com.z4greed.inventory.entity.InventoryMovementEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryMovementRepository extends JpaRepository<InventoryMovementEntity, Long> {}
