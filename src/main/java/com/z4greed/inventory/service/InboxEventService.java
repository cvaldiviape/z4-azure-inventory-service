package com.z4greed.inventory.service;

import com.z4greed.inventory.kafka.event.EventEnvelopeDto;

public interface InboxEventService {
  boolean wasAlreadyProcessed(String eventId);

  void register(EventEnvelopeDto eventEnvelopeDto, String sourceTopic);
}
