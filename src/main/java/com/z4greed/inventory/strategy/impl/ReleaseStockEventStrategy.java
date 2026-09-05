package com.z4greed.inventory.strategy.impl;

import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.kafka.factory.InventoryEventFactory;
import com.z4greed.inventory.service.StockService;
import com.z4greed.inventory.strategy.StockEventStrategy;
import com.z4greed.inventory.service.OutboxEventService;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ReleaseStockEventStrategy implements StockEventStrategy {
  private final StockService stockService;
  private final InventoryEventFactory inventoryEventFactory;
  private final OutboxEventService outboxEventService;

  public ReleaseStockEventStrategy(
      StockService stockService,
      InventoryEventFactory inventoryEventFactory,
      OutboxEventService outboxEventService
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
  public void execute(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());

    List<ReservationEntity> listReservations = this.stockService.findReservedByOrderId(orderId);
    this.stockService.releaseAll(listReservations);

    Map<String, Object> mapPayload = Map.of();
    EventEnvelopeDto stockReleasedEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RELEASED, eventEnvelopeDto, mapPayload);

    this.outboxEventService.enqueue("inventory-events-topic", stockReleasedEvent);
  }

}
