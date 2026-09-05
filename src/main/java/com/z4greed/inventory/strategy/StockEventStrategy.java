package com.z4greed.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;

public interface StockEventStrategy {
  EventTypeEnum getEventType();
  void execute(EventEnvelopeDto eventEnvelopeDto);
}
