package com.z4greed.inventory.entity;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.*;
import lombok.AccessLevel;

@Entity
@Table(name = "inventory")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class InventoryEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private Long productId;
  private int availableQuantity;
  private int reservedQuantity;
  @Version private Long version;
  private Instant updatedAt;
}
