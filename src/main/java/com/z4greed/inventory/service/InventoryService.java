package com.z4greed.inventory.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.inventory.entity.ProcessedEventEntity;
import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.enums.ReservationStatusEnum;
import com.z4greed.inventory.exception.GreedException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.kafka.producer.InventoryEventProducer;
import com.z4greed.inventory.repository.InventoryRepository;
import com.z4greed.inventory.repository.ProcessedEventRepository;
import com.z4greed.inventory.repository.ReservationRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class InventoryService {
  private final InventoryRepository inventoryRepository;
  private final ReservationRepository reservationRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final InventoryEventProducer inventoryEventProducer;
  private final ObjectMapper objectMapper;
  private final Map<EventTypeEnum, Consumer<EventEnvelopeDto>> mapEventHandlers;

  public InventoryService(
      InventoryRepository inventoryRepository,
      ReservationRepository reservationRepository,
      ProcessedEventRepository processedEventRepository,
      InventoryEventProducer inventoryEventProducer,
      ObjectMapper objectMapper) {
    this.inventoryRepository = inventoryRepository;
    this.reservationRepository = reservationRepository;
    this.processedEventRepository = processedEventRepository;
    this.inventoryEventProducer = inventoryEventProducer;
    this.objectMapper = objectMapper;
    this.mapEventHandlers = Map.of(EventTypeEnum.ORDER_CREATED, this::reserveStock,
        EventTypeEnum.RELEASE_STOCK, this::releaseStock);
  }

  public void process(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    if (this.processedEventRepository.existsById(eventEnvelopeDto.eventId())) {
      return;
    }

    Consumer<EventEnvelopeDto> eventHandler = EventTypeEnum.fromValue(eventEnvelopeDto.eventType())
        .map(this.mapEventHandlers::get)
        .orElse(null);
    if (eventHandler == null) {
      return;
    }

    eventHandler.accept(eventEnvelopeDto);
    ProcessedEventEntity processedEventEntity = new ProcessedEventEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

  private void reserveStock(EventEnvelopeDto sourceEvent) {
    Long orderId = Long.valueOf(sourceEvent.aggregateId());
    List<ReservationEntity> listCreatedReservations = new ArrayList<>();
    for (JsonNode itemNode : sourceEvent.payload().get("items")) {
      Long productId = itemNode.get("productId").asLong();
      int quantity = itemNode.get("quantity").asInt();
      int affectedRows = inventoryRepository.reserve(productId, quantity);
      if (affectedRows == 0) {
        rollbackReservations(listCreatedReservations);
        Map<String, Object> mapPayload = Map.of("productId", productId);
        this.publishEvent(sourceEvent, EventTypeEnum.STOCK_NOT_AVAILABLE.getValue(), mapPayload);
        return;
      }
      ReservationEntity reservationEntity =
          ReservationEntity.builder()
              .orderId(orderId)
              .productId(productId)
              .quantity(quantity)
              .build();
      this.reservationRepository.save(reservationEntity);
      listCreatedReservations.add(reservationEntity);
    }
    List<String> listReservationIds =
        listCreatedReservations.stream().map(ReservationEntity::getReservationId).toList();
    Map<String, Object> mapPayload = Map.of("reservationIds", listReservationIds);
    this.publishEvent(sourceEvent, EventTypeEnum.STOCK_RESERVED.getValue(), mapPayload);
  }

  private void rollbackReservations(List<ReservationEntity> listReservations) {
    listReservations.forEach(
        reservationEntity -> {
          Long productId = reservationEntity.getProductId();
          int quantity = reservationEntity.getQuantity();
          this.inventoryRepository.release(productId, quantity);
          reservationEntity.release();
        });
  }

  private void releaseStock(EventEnvelopeDto sourceEvent) {
    Long orderId = Long.valueOf(sourceEvent.aggregateId());
    List<ReservationEntity> listReservations =
        this.reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatusEnum.RESERVED);
    listReservations.forEach(
        reservationEntity -> {
          Long productId = reservationEntity.getProductId();
          int quantity = reservationEntity.getQuantity();
          int affectedRows = inventoryRepository.release(productId, quantity);
          if (affectedRows != 1) {
            throw new GreedException(ErrorCodeEnum.INVALID_RESERVED_STOCK);
          }
          reservationEntity.release();
        });
    Map<String, Object> mapPayload = Map.of();
    this.publishEvent(sourceEvent, EventTypeEnum.STOCK_RELEASED.getValue(), mapPayload);
  }

  private void publishEvent(EventEnvelopeDto sourceEvent, String eventType, Object payload) {
    String eventId = UUID.randomUUID().toString();
    EventEnvelopeDto eventEnvelopeDto =
        EventEnvelopeDto.builder()
            .eventId(eventId)
            .eventType(eventType)
            .eventVersion(1)
            .aggregateId(sourceEvent.aggregateId())
            .correlationId(sourceEvent.correlationId())
            .causationId(sourceEvent.eventId())
            .timestamp(Instant.now())
            .producer("inventory-service")
            .payload(objectMapper.valueToTree(payload))
            .build();
    this.inventoryEventProducer.publish(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return objectMapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }
}
