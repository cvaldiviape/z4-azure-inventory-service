package com.z4greed.inventory.service.inventory.strategy.impl;

import tools.jackson.databind.JsonNode;
import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.kafka.factory.InventoryEventFactory;
import com.z4greed.inventory.kafka.producer.InventoryEventProducer;
import com.z4greed.inventory.service.inventory.reservation.InventoryReservationManager;
import com.z4greed.inventory.service.inventory.strategy.InventoryEventStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ReserveStockEventStrategy implements InventoryEventStrategy {
  private final InventoryReservationManager inventoryReservationManager;
  private final InventoryEventFactory inventoryEventFactory;
  private final InventoryEventProducer inventoryEventProducer;

  public ReserveStockEventStrategy(
      InventoryReservationManager inventoryReservationManager,
      InventoryEventFactory inventoryEventFactory,
      InventoryEventProducer inventoryEventProducer
  ) {
    this.inventoryReservationManager = inventoryReservationManager;
    this.inventoryEventFactory = inventoryEventFactory;
    this.inventoryEventProducer = inventoryEventProducer;
  }

  @Override
  public EventTypeEnum getEventType() {
    return EventTypeEnum.RESERVE_STOCK;
  }

  @Override
  public void execute(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    List<ReservationEntity> listReservations = new ArrayList<>();
    JsonNode listItems = eventEnvelopeDto.payload().get("items");

    for (JsonNode itemNode : listItems) {
      boolean reserved = this.inventoryReservationManager.tryReserve(orderId, itemNode, listReservations);

      if (!reserved) {
        this.publishStockNotAvailable(eventEnvelopeDto, itemNode);
        return;
      }
    }

    this.publishStockReserved(eventEnvelopeDto, listReservations);
  }

  private void publishStockNotAvailable(EventEnvelopeDto sourceEvent, JsonNode itemNode) {
    Long productId = itemNode.get("productId").asLong();
    Map<String, Object> mapPayload = Map.of("productId", productId);

    EventEnvelopeDto eventEnvelopeDto = this.inventoryEventFactory.build(EventTypeEnum.STOCK_NOT_AVAILABLE, sourceEvent, mapPayload);
    this.inventoryEventProducer.publish("inventory-events-topic", eventEnvelopeDto);
  }

  private void publishStockReserved(EventEnvelopeDto sourceEvent, List<ReservationEntity> listReservations) {
    List<String> listReservationIds = listReservations.stream()
        .map(ReservationEntity::getReservationId)
        .toList();
    Map<String, Object> mapPayload = Map.of("reservationIds", listReservationIds);

    EventEnvelopeDto eventEnvelopeDto = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RESERVED, sourceEvent, mapPayload);

    this.inventoryEventProducer.publish("inventory-events-topic", eventEnvelopeDto);
  }

}