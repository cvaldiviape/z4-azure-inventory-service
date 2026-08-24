package com.z4greed.inventory.dto;

import com.z4greed.inventory.enums.ReservationStatusEnum;
import java.time.Instant;
import lombok.Builder;

@Builder
public record ReservationCreateDto(
    String reservationId,
    Long orderId,
    Long productId,
    int quantity,
    ReservationStatusEnum status,
    Instant createdAt,
    Instant updatedAt) {}
