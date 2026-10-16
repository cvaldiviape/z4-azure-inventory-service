package com.z4greed.inventory.kafka.factory;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.IntegrationEventDto;
import java.time.LocalDateTime;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventFactory {
  private static final Integer EVENT_VERSION = 1;
  private static final String PRODUCER = "inventory-service";

  private final ObjectMapper objectMapper;

  public InventoryEventFactory(
      ObjectMapper objectMapper
  ) {
    this.objectMapper = objectMapper;
  }

  public IntegrationEventDto build(EventTypeEnum eventTypeEnum, IntegrationEventDto event, Object payload) {
    return IntegrationEventDto.builder()
        .eventId(UUID.randomUUID().toString())
        .eventType(eventTypeEnum.getValue())
        .eventVersion(EVENT_VERSION)
        .aggregateId(event.aggregateId())
        .correlationId(event.correlationId())
        .previousEventId(event.eventId())
        .timestamp(LocalDateTime.now())
        .producer(PRODUCER)
        .payload(this.objectMapper.valueToTree(payload))
        .build();
  }
}
