package com.z4greed.inventory.service.inventory.reservation;

import tools.jackson.databind.JsonNode;
import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.ReservationStatusEnum;
import com.z4greed.inventory.exception.GreedException;
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

  public Boolean reserve(Long orderId, JsonNode itemNode, List<ReservationEntity> listReservations) {
    Long productId = itemNode.get("productId").asLong();
    Integer quantity = itemNode.get("quantity").asInt();
    Integer affectedRows = this.inventoryRepository.reserve(productId, quantity);

    if (affectedRows == 0) {
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
    listReservations.forEach(this::release);
  }

  private void release(ReservationEntity reservationEntity) {
    Long productId = reservationEntity.getProductId();
    Integer quantity = reservationEntity.getQuantity();
    Integer affectedRows = this.inventoryRepository.release(productId, quantity);

    if (affectedRows != 1) {
      throw new GreedException(ErrorCodeEnum.INVALID_RESERVED_STOCK);
    }

    reservationEntity.setStatus(ReservationStatusEnum.RELEASED);
    reservationEntity.setUpdatedAt(LocalDateTime.now());
  }
}
