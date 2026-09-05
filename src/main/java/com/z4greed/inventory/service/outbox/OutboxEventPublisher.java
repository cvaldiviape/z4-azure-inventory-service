package com.z4greed.inventory.service.outbox;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.kafka.producer.InventoryEventProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OutboxEventPublisher {
  private final OutboxEventService outboxEventService;
  private final InventoryEventProducer inventoryEventProducer;
  private final int batchSize;

  public OutboxEventPublisher(
      OutboxEventService outboxEventService,
      InventoryEventProducer inventoryEventProducer,
      @Value("${app.outbox.batch-size}") int batchSize) {
    this.outboxEventService = outboxEventService;
    this.inventoryEventProducer = inventoryEventProducer;
    this.batchSize = batchSize;
  }

  // PENDING existe únicamente en PostgreSQL. Este componente consulta esos registros
  // periódicamente, los publica en Kafka y solo después del ACK los marca PUBLISHED.
  @Scheduled(
      fixedDelayString = "${app.outbox.poll-interval-milliseconds}",
      initialDelayString = "${app.outbox.initial-delay-milliseconds}")
  public void publishPendingEvents() {
    List<OutboxEventEntity> pendingEvents = this.outboxEventService.findPending(this.batchSize);

    for (OutboxEventEntity outboxEvent : pendingEvents) {
      try {
        this.inventoryEventProducer.publishAndWait(outboxEvent);
        this.outboxEventService.markAsPublished(outboxEvent.getEventId());
      } catch (RuntimeException exception) {
        this.outboxEventService.registerFailedAttempt(
            outboxEvent.getEventId(),
            this.limitErrorMessage(exception.getMessage()));
        log.error(
            "action=outbox_publish_failed eventType={} eventId={} correlationId={} orderId={} attempts={}",
            outboxEvent.getEventType(),
            outboxEvent.getEventId(),
            outboxEvent.getCorrelationId(),
            outboxEvent.getAggregateId(),
            outboxEvent.getAttempts() + 1,
            exception);
      }
    }
  }

  private String limitErrorMessage(String errorMessage) {
    if (errorMessage == null) {
      return "Unknown error";
    }
    return errorMessage.substring(0, Math.min(errorMessage.length(), 2000));
  }
}
