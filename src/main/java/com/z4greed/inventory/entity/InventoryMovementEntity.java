package com.z4greed.inventory.entity;

import com.z4greed.inventory.enums.InventoryMovementTypeEnum;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "inventory_movements")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class InventoryMovementEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private Long productId;
  private Long orderId;
  private String reservationId;
  private String eventId;

  @Enumerated(EnumType.STRING)
  private InventoryMovementTypeEnum movementType;

  private Integer quantity;
  private Integer availableBefore;
  private Integer availableAfter;
  private Integer reservedBefore;
  private Integer reservedAfter;
  private LocalDateTime createdAt;
}
