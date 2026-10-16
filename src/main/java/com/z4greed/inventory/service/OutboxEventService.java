package com.z4greed.inventory.service;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.kafka.event.IntegrationEventDto;
import java.util.List;

public interface OutboxEventService {
  void enqueue(String topic, IntegrationEventDto event);

  List<OutboxEventEntity> findPending(int batchSize);

  void markAsPublished(String eventId);

  void registerFailedAttempt(String eventId, String errorMessage);
}
