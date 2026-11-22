package com.z4greed.inventory.command.impl;

import com.z4greed.inventory.command.InventoryCommandService;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.KafkaTopics;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.event.IntegrationEventReader;
import com.z4greed.shared.kafka.inbox.InboxEventService;
import com.z4greed.inventory.strategy.EventStrategy;
import com.z4greed.inventory.strategy.StockEventStrategyRegistry;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class InventoryCommandServiceImpl implements InventoryCommandService {
    private final InboxEventService inboxEventService;
    private final StockEventStrategyRegistry stockEventStrategyRegistry;
    private final IntegrationEventReader integrationEventReader;

    public InventoryCommandServiceImpl(
            InboxEventService inboxEventService,
            StockEventStrategyRegistry stockEventStrategyRegistry,
            IntegrationEventReader integrationEventReader
    ) {
        this.inboxEventService = inboxEventService;
        this.stockEventStrategyRegistry = stockEventStrategyRegistry;
        this.integrationEventReader = integrationEventReader;
    }

    // Los comandos duplicados o de un tipo que este servicio no atiende se ignoran sin registrarse en el inbox.
    @Override
    public void process(String rawEvent) {
        IntegrationEventDto event = this.integrationEventReader.read(rawEvent);

        if (this.inboxEventService.wasAlreadyProcessed(event.eventId())) {
            return;
        }

        this.findStrategy(event.eventType()).ifPresent(stockEventStrategy -> {
            stockEventStrategy.execute(event);
            this.inboxEventService.register(event, KafkaTopics.INVENTORY_RESERVATION_REQUESTS);
        });
    }

    private Optional<EventStrategy<EventTypeEnum, IntegrationEventDto>> findStrategy(String eventType) {
        return EventTypeEnum.fromValue(eventType).map(this.stockEventStrategyRegistry::find);
    }

}
