package com.z4greed.inventory.kafka.factory;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.event.IntegrationEventFactory;
import org.springframework.stereotype.Component;

// Adapta la fábrica genérica de z4-shared-kafka a los tipos de evento de Inventory Service.
@Component
public class InventoryEventFactory {
    private final IntegrationEventFactory integrationEventFactory;

    public InventoryEventFactory(IntegrationEventFactory integrationEventFactory) {
        this.integrationEventFactory = integrationEventFactory;
    }

    public IntegrationEventDto build(EventTypeEnum eventTypeEnum, IntegrationEventDto previousEvent) {
        String eventType = eventTypeEnum.getValue();

        return this.integrationEventFactory.create(eventType, previousEvent);
    }

    public IntegrationEventDto build(EventTypeEnum eventTypeEnum, IntegrationEventDto previousEvent, Object payload) {
        String eventType = eventTypeEnum.getValue();

        return this.integrationEventFactory.create(eventType, previousEvent, payload);
    }
}
