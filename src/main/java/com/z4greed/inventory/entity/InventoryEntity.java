package com.z4greed.inventory.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;
import lombok.AccessLevel;

@Entity
@Table(name = "inventories")
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
  private Integer availableQuantity;
  private Integer reservedQuantity;
  @Version private Long version;
  private LocalDateTime updatedAt;
}
