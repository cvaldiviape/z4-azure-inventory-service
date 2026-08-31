package com.z4greed.inventory.service.inventory.strategy.impl;

import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.kafka.factory.InventoryEventFactory;
import com.z4greed.inventory.kafka.producer.InventoryEventProducer;
import com.z4greed.inventory.service.inventory.reservation.InventoryReservationManager;
import com.z4greed.inventory.service.inventory.strategy.InventoryEventStrategy;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ReleaseStockEventStrategy implements InventoryEventStrategy {
  private final InventoryReservationManager inventoryReservationManager;
  private final InventoryEventFactory inventoryEventFactory;
  private final InventoryEventProducer inventoryEventProducer;

  public ReleaseStockEventStrategy(
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
    return EventTypeEnum.RELEASE_STOCK;
  }

  @Override
  public void execute(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());

    List<ReservationEntity> listReservations = this.inventoryReservationManager.findReservedByOrderId(orderId);
    this.inventoryReservationManager.releaseAll(listReservations);

    Map<String, Object> mapPayload = Map.of();
    EventEnvelopeDto stockReleasedEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RELEASED, eventEnvelopeDto, mapPayload);

    this.inventoryEventProducer.publish("inventory-events-topic", stockReleasedEvent);
  }

}
