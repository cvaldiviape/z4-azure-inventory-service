package com.z4greed.inventory.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.inventory.dto.ReservationCreateDto;
import com.z4greed.inventory.entity.*;
import com.z4greed.inventory.enums.*;
import com.z4greed.inventory.exception.GreedException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.kafka.producer.InventoryEventProducer;
import com.z4greed.inventory.mapper.*;
import com.z4greed.inventory.repository.*;
import com.z4greed.inventory.service.InventoryService;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class InventoryServiceImpl implements InventoryService {
  private final InventoryRepository inventoryRepository;
  private final ReservationRepository reservationRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final InventoryEventProducer inventoryEventProducer;
  private final InventoryMapper inventoryMapper;
  private final ProcessedEventMapper processedEventMapper;
  private final ObjectMapper objectMapper;

  public InventoryServiceImpl(
      InventoryRepository inventoryRepository,
      ReservationRepository reservationRepository,
      ProcessedEventRepository processedEventRepository,
      InventoryEventProducer inventoryEventProducer,
      InventoryMapper inventoryMapper,
      ProcessedEventMapper processedEventMapper,
      ObjectMapper objectMapper) {
    this.inventoryRepository = inventoryRepository;
    this.reservationRepository = reservationRepository;
    this.processedEventRepository = processedEventRepository;
    this.inventoryEventProducer = inventoryEventProducer;
    this.inventoryMapper = inventoryMapper;
    this.processedEventMapper = processedEventMapper;
    this.objectMapper = objectMapper;
  }

  @Override
  public void process(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    if (this.wasProcessed(eventEnvelopeDto)) {
      return;
    }

    Consumer<EventEnvelopeDto> eventHandler = this.findEventHandler(eventEnvelopeDto);
    if (eventHandler == null) {
      return;
    }

    eventHandler.accept(eventEnvelopeDto);
    this.markAsProcessed(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.objectMapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    return this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
  }

  private Consumer<EventEnvelopeDto> findEventHandler(EventEnvelopeDto eventEnvelopeDto) {
    Map<EventTypeEnum, Consumer<EventEnvelopeDto>> mapEventHandlers = Map.of(
        EventTypeEnum.ORDER_CREATED, this::reserveStock,
        EventTypeEnum.RELEASE_STOCK, this::releaseStock);
    return EventTypeEnum.fromValue(eventEnvelopeDto.eventType())
        .map(mapEventHandlers::get)
        .orElse(null);
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

  private void reserveStock(EventEnvelopeDto sourceEvent) {
    Long orderId = Long.valueOf(sourceEvent.aggregateId());
    List<ReservationEntity> listReservations = new ArrayList<>();
    for (JsonNode itemNode : sourceEvent.payload().get("items")) {
      Boolean reserved = this.reserveItem(orderId, itemNode, listReservations);
      if (!reserved) {
        this.publishStockNotAvailable(sourceEvent, itemNode);
        return;
      }
    }
    this.publishStockReserved(sourceEvent, listReservations);
  }

  private Boolean reserveItem(Long orderId, JsonNode itemNode, List<ReservationEntity> listReservations) {
    Long productId = itemNode.get("productId").asLong();
    Integer quantity = itemNode.get("quantity").asInt();
    Integer affectedRows = this.inventoryRepository.reserve(productId, quantity);
    if (affectedRows == 0) {
      this.rollbackReservations(listReservations);
      return false;
    }

    ReservationEntity reservationEntity = this.createReservation(orderId, productId, quantity);
    listReservations.add(reservationEntity);
    return true;
  }

  private ReservationEntity createReservation(Long orderId, Long productId, Integer quantity) {
    ReservationCreateDto reservationCreateDto = ReservationCreateDto.builder()
        .reservationId(UUID.randomUUID().toString())
        .orderId(orderId)
        .productId(productId)
        .quantity(quantity)
        .status(ReservationStatusEnum.RESERVED)
        .createdAt(LocalDateTime.now())
        .build();
    ReservationEntity reservationEntity = this.inventoryMapper.toEntity(reservationCreateDto);
    return this.reservationRepository.save(reservationEntity);
  }

  private void rollbackReservations(List<ReservationEntity> listReservations) {
    listReservations.forEach(this::releaseReservation);
  }

  private void publishStockNotAvailable(EventEnvelopeDto sourceEvent, JsonNode itemNode) {
    Long productId = itemNode.get("productId").asLong();
    Map<String, Object> mapPayload = Map.of("productId", productId);
    this.publishEvent(sourceEvent, EventTypeEnum.STOCK_NOT_AVAILABLE, mapPayload);
  }

  private void publishStockReserved(EventEnvelopeDto sourceEvent, List<ReservationEntity> listReservations) {
    List<String> listReservationIds = listReservations.stream()
        .map(ReservationEntity::getReservationId)
        .toList();
    Map<String, Object> mapPayload = Map.of("reservationIds", listReservationIds);
    this.publishEvent(sourceEvent, EventTypeEnum.STOCK_RESERVED, mapPayload);
  }

  private void releaseStock(EventEnvelopeDto sourceEvent) {
    Long orderId = Long.valueOf(sourceEvent.aggregateId());
    List<ReservationEntity> listReservations = this.reservationRepository
        .findByOrderIdAndStatus(orderId, ReservationStatusEnum.RESERVED);
    listReservations.forEach(this::releaseReservation);
    Map<String, Object> mapPayload = Map.of();
    this.publishEvent(sourceEvent, EventTypeEnum.STOCK_RELEASED, mapPayload);
  }

  private void releaseReservation(ReservationEntity reservationEntity) {
    Long productId = reservationEntity.getProductId();
    Integer quantity = reservationEntity.getQuantity();
    Integer affectedRows = this.inventoryRepository.release(productId, quantity);
    if (affectedRows != 1) {
      throw new GreedException(ErrorCodeEnum.INVALID_RESERVED_STOCK);
    }
    reservationEntity.setStatus(ReservationStatusEnum.RELEASED);
    reservationEntity.setUpdatedAt(LocalDateTime.now());
  }

  private void publishEvent(EventEnvelopeDto sourceEvent, EventTypeEnum eventType, Object payload) {
    EventEnvelopeDto eventEnvelopeDto = EventEnvelopeDto.builder()
        .eventId(UUID.randomUUID().toString())
        .eventType(eventType.getValue())
        .eventVersion(1)
        .aggregateId(sourceEvent.aggregateId())
        .correlationId(sourceEvent.correlationId())
        .causationId(sourceEvent.eventId())
        .timestamp(LocalDateTime.now())
        .producer("inventory-service")
        .payload(this.objectMapper.valueToTree(payload))
        .build();
    this.inventoryEventProducer.publish(eventEnvelopeDto);
  }
}
