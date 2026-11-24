package com.z4greed.inventory.strategy.impl;

import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.inventory.kafka.KafkaTopics;
import com.z4greed.inventory.kafka.factory.InventoryEventFactory;
import com.z4greed.inventory.service.StockService;
import com.z4greed.inventory.strategy.EventStrategy;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import java.util.List;
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

        IntegrationEventDto stockReleasedEvent = this.inventoryEventFactory.build(EventTypeEnum.STOCK_RELEASED, event);

        this.outboxEventService.enqueue(KafkaTopics.INVENTORY_RESERVATION_RESULTS, stockReleasedEvent);
    }

}
