package com.z4greed.inventory.strategy.impl;

import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.IntegrationEventDto;
import com.z4greed.inventory.kafka.factory.InventoryEventFactory;
import com.z4greed.inventory.service.StockService;
import com.z4greed.inventory.strategy.EventStrategy;
import com.z4greed.inventory.service.OutboxEventService;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ReleaseStockEventStrategy implements EventStrategy<EventTypeEnum, IntegrationEventDto> {
  private final StockService stockService;
  private final OutboxEventService outboxEventService;
  private final InventoryEventFactory inventoryEventFactory;

  public ReleaseStockEventStrategy(
      StockService stockService,
      OutboxEventService outboxEventService,
      InventoryEventFactory inventoryEventFactory
  ) {
    this.stockService = stockService;
    this.inventoryEventFactory = inventoryEventFactory;
    this.outboxEventService = outboxEventService;
  }

  @Override
  public EventTypeEnum getEventType() {
    return EventTypeEnum.RELEASE_STOCK;
  }

  @Override
  public void execute(IntegrationEventDto event) {
    Long orderId = Long.valueOf(event.aggregateId());

    List<ReservationEntity> listReservations = this.stockService.findReservedByOrderId(orderId);
    this.stockService.releaseAll(listReservations, event.eventId());

    Map<String, Object> mapPayload = Map.of();
    IntegrationEventDto stockReleasedEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RELEASED, event, mapPayload);

    this.outboxEventService.enqueue("inventory-reservation-results-topic", stockReleasedEvent);
  }

}
