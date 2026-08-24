package com.z4greed.inventory.entity;

import com.z4greed.inventory.enums.ReservationStatusEnum;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import lombok.*;

@Entity
@Table(name = "inventory_reservations")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ReservationEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String reservationId;
  private Long orderId;
  private Long productId;
  private int quantity;

  @Enumerated(EnumType.STRING)
  private ReservationStatusEnum status;

  private Instant createdAt;
  private Instant updatedAt;

  @Builder
  public ReservationEntity(Long orderId, Long productId, int quantity) {
    this.reservationId = UUID.randomUUID().toString();
    this.orderId = orderId;
    this.productId = productId;
    this.quantity = quantity;
    this.status = ReservationStatusEnum.RESERVED;
    this.createdAt = Instant.now();
  }

  public void release() {
    this.status = ReservationStatusEnum.RELEASED;
    this.updatedAt = Instant.now();
  }
}
