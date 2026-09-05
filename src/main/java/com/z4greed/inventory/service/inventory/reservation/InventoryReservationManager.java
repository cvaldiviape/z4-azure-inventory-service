package com.z4greed.inventory.service.inventory.reservation;

import tools.jackson.databind.JsonNode;
import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.ReservationStatusEnum;
import com.z4greed.inventory.exception.CustomBusinessException;
import com.z4greed.inventory.repository.InventoryRepository;
import com.z4greed.inventory.repository.ReservationRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class InventoryReservationManager {
  private final InventoryRepository inventoryRepository;
  private final ReservationRepository reservationRepository;

  public InventoryReservationManager(
      InventoryRepository inventoryRepository,
      ReservationRepository reservationRepository
  ) {
    this.inventoryRepository = inventoryRepository;
    this.reservationRepository = reservationRepository;
  }

  public boolean tryReserve(Long orderId, JsonNode itemNode, List<ReservationEntity> listReservations) {
    Long productId = itemNode.get("productId").asLong();
    Integer quantity = itemNode.get("quantity").asInt();
    int updatedRows = this.inventoryRepository.reserveIfAvailable(productId, quantity);

    if (updatedRows == 0) {
      this.releaseAll(listReservations);
      return false;
    }

    ReservationEntity reservationEntity = this.create(orderId, productId, quantity);
    listReservations.add(reservationEntity);
    return true;
  }

  private ReservationEntity create(Long orderId, Long productId, Integer quantity) {
    ReservationEntity reservationEntity = ReservationEntity.builder()
        .reservationId(UUID.randomUUID().toString())
        .orderId(orderId)
        .productId(productId)
        .quantity(quantity)
        .status(ReservationStatusEnum.RESERVED)
        .createdAt(LocalDateTime.now())
        .build();

    return this.reservationRepository.save(reservationEntity);
  }

  public List<ReservationEntity> findReservedByOrderId(Long orderId) {
    return this.reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatusEnum.RESERVED);
  }

  public void releaseAll(List<ReservationEntity> listReservations) {
    listReservations.forEach(this::releaseReservation);
  }

  private void releaseReservation(ReservationEntity reservationEntity) {
    Long productId = reservationEntity.getProductId();
    Integer quantity = reservationEntity.getQuantity();
    int updatedRows = this.inventoryRepository.releaseIfReserved(productId, quantity);

    if (updatedRows != 1) {
      throw new CustomBusinessException(ErrorCodeEnum.INVALID_RESERVED_STOCK);
    }

    reservationEntity.setStatus(ReservationStatusEnum.RELEASED);
    reservationEntity.setUpdatedAt(LocalDateTime.now());
  }
}
