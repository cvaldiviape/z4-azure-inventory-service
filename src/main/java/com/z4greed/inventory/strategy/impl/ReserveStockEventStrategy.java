package com.z4greed.inventory.strategy.impl;

import tools.jackson.databind.JsonNode;
import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.IntegrationEventDto;
import com.z4greed.inventory.kafka.factory.InventoryEventFactory;
import com.z4greed.inventory.service.StockService;
import com.z4greed.inventory.strategy.EventStrategy;
import com.z4greed.inventory.service.OutboxEventService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ReserveStockEventStrategy implements EventStrategy<EventTypeEnum, IntegrationEventDto> {
  private final StockService stockService;
  private final OutboxEventService outboxEventService;
  private final InventoryEventFactory inventoryEventFactory;

  public ReserveStockEventStrategy(
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
    return EventTypeEnum.RESERVE_STOCK;
  }

  @Override
  public void execute(IntegrationEventDto event) {
    Long orderId = Long.valueOf(event.aggregateId());
    List<ReservationEntity> listReservations = new ArrayList<>();
    JsonNode listItems = event.payload().get("items");

    for (JsonNode itemNode : listItems) {
      boolean reserved = this.stockService.tryReserve(orderId, itemNode, listReservations, event.eventId());

      if (!reserved) {
        this.publishStockNotAvailable(event, itemNode);
        return;
      }
    }
    this.publishStockReserved(event, listReservations);
  }

  private void publishStockNotAvailable(IntegrationEventDto event, JsonNode itemNode) {
    Long productId = itemNode.get("productId").asLong();
    Map<String, Object> mapPayload = Map.of(
            "productId", productId
    );

    IntegrationEventDto resultEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_NOT_AVAILABLE, event, mapPayload);

    this.outboxEventService.enqueue("inventory-reservation-results-topic", resultEvent);
  }

  private void publishStockReserved(IntegrationEventDto event, List<ReservationEntity> listReservations) {
    List<String> listReservationIds = listReservations.stream()
        .map(ReservationEntity::getReservationId)
        .toList();
    Map<String, Object> mapPayload = Map.of(
            "reservationIds", listReservationIds
    );

    IntegrationEventDto resultEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RESERVED, event, mapPayload);

    this.outboxEventService.enqueue("inventory-reservation-results-topic", resultEvent);
  }

}
