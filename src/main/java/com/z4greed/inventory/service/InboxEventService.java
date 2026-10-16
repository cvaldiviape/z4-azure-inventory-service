package com.z4greed.inventory.service;

import com.z4greed.inventory.kafka.event.IntegrationEventDto;

public interface InboxEventService {
  boolean wasAlreadyProcessed(String eventId);

  void register(IntegrationEventDto event, String sourceTopic);
}
