package com.z4greed.inventory.service.impl;

import com.z4greed.inventory.entity.InventoryEntity;
import com.z4greed.inventory.entity.InventoryMovementEntity;
import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.InventoryMovementTypeEnum;
import com.z4greed.inventory.enums.ReservationStatusEnum;
import com.z4greed.inventory.exception.CustomBusinessException;
import com.z4greed.inventory.repository.InventoryRepository;
import com.z4greed.inventory.repository.InventoryMovementRepository;
import com.z4greed.inventory.repository.ReservationRepository;
import com.z4greed.inventory.service.StockService;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

@Service
public class StockServiceImpl implements StockService {
  private final InventoryRepository inventoryRepository;
  private final InventoryMovementRepository inventoryMovementRepository;
  private final ReservationRepository reservationRepository;

  public StockServiceImpl(
      InventoryRepository inventoryRepository,
      InventoryMovementRepository inventoryMovementRepository,
      ReservationRepository reservationRepository
  ) {
    this.inventoryRepository = inventoryRepository;
    this.inventoryMovementRepository = inventoryMovementRepository;
    this.reservationRepository = reservationRepository;
  }

  @Override
  public boolean tryReserve(
      Long orderId,
      JsonNode itemNode,
      List<ReservationEntity> listReservations,
      String eventId
  ) {
    Long productId = itemNode.get("productId").asLong();
    Integer quantity = itemNode.get("quantity").asInt();
    int updatedRows = this.inventoryRepository.reserveIfAvailable(productId, quantity);

    if (updatedRows == 0) {
      this.releaseAll(listReservations, eventId);
      return false;
    }

    ReservationEntity reservationEntity = this.create(orderId, productId, quantity);
    this.registerMovement(reservationEntity, eventId, InventoryMovementTypeEnum.RESERVATION);
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

  @Override
  public List<ReservationEntity> findReservedByOrderId(Long orderId) {
    return this.reservationRepository.findByOrderIdAndStatus(
        orderId,
        ReservationStatusEnum.RESERVED);
  }

  @Override
  public void releaseAll(List<ReservationEntity> listReservations, String eventId) {
    listReservations.forEach(reservationEntity -> this.releaseReservation(reservationEntity, eventId));
  }

  private void releaseReservation(ReservationEntity reservationEntity, String eventId) {
    Long productId = reservationEntity.getProductId();
    Integer quantity = reservationEntity.getQuantity();
    int updatedRows = this.inventoryRepository.releaseIfReserved(productId, quantity);

    if (updatedRows != 1) {
      throw new CustomBusinessException(ErrorCodeEnum.INVALID_RESERVED_STOCK);
    }

    reservationEntity.setStatus(ReservationStatusEnum.RELEASED);
    reservationEntity.setUpdatedAt(LocalDateTime.now());
    this.reservationRepository.save(reservationEntity);
    this.registerMovement(reservationEntity, eventId, InventoryMovementTypeEnum.RELEASE);
  }

  private void registerMovement(
      ReservationEntity reservationEntity,
      String eventId,
      InventoryMovementTypeEnum movementType
  ) {
    Integer quantity = reservationEntity.getQuantity();
    InventoryEntity inventoryEntity = this.inventoryRepository.findByProductId(reservationEntity.getProductId())
        .orElseThrow(() -> new IllegalStateException("Inventory not found for product " + reservationEntity.getProductId()));

    boolean isReservation = movementType == InventoryMovementTypeEnum.RESERVATION;
    Integer availableAfter = inventoryEntity.getAvailableQuantity();
    Integer reservedAfter = inventoryEntity.getReservedQuantity();

    InventoryMovementEntity movementEntity = InventoryMovementEntity.builder()
        .productId(reservationEntity.getProductId())
        .orderId(reservationEntity.getOrderId())
        .reservationId(reservationEntity.getReservationId())
        .eventId(eventId)
        .movementType(movementType)
        .quantity(quantity)
        .availableBefore(isReservation ? availableAfter + quantity : availableAfter - quantity)
        .availableAfter(availableAfter)
        .reservedBefore(isReservation ? reservedAfter - quantity : reservedAfter + quantity)
        .reservedAfter(reservedAfter)
        .createdAt(LocalDateTime.now())
        .build();

    this.inventoryMovementRepository.save(movementEntity);
  }
}
