package com.z4greed.inventory.service.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;

public interface InventoryEventStrategy {
  EventTypeEnum getEventType();
  void execute(EventEnvelopeDto eventEnvelopeDto);
}
