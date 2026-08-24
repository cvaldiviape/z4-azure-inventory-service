package com.z4greed.inventory.entity;

import com.z4greed.inventory.enums.ReservationStatusEnum;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "inventory_reservations")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ReservationEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String reservationId;
  private Long orderId;
  private Long productId;
  private Integer quantity;

  @Enumerated(EnumType.STRING)
  private ReservationStatusEnum status;

  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
}
