package com.z4greed.inventory.dto;

import com.z4greed.inventory.enums.ReservationStatusEnum;
import java.time.LocalDateTime;
import lombok.Builder;

@Builder
public record ReservationCreateDto(
    String reservationId,
    Long orderId,
    Long productId,
    Integer quantity,
    ReservationStatusEnum status,
    LocalDateTime createdAt,
    LocalDateTime updatedAt) {}
