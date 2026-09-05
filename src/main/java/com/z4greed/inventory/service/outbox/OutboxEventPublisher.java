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
      @Value("${app.outbox.batch-size}") int batchSize
  ) {
    this.outboxEventService = outboxEventService;
    this.inventoryEventProducer = inventoryEventProducer;
    this.batchSize = batchSize;
  }

  // PENDING existe únicamente en PostgreSQL. Este componente consulta esos registros
  // periódicamente, los publica en Kafka y solo después del ACK los marca PUBLISHED.
  @Scheduled(fixedDelayString = "${app.outbox.poll-interval-milliseconds}", initialDelayString = "${app.outbox.initial-delay-milliseconds}")
  public void publishPendingEvents() {
    List<OutboxEventEntity> listPendingEvents = this.outboxEventService.findPending(this.batchSize);

    for (OutboxEventEntity outboxEvent : listPendingEvents) {
      this.publishPendingEvent(outboxEvent);
    }
  }

  private void publishPendingEvent(OutboxEventEntity outboxEvent) {
    String eventId = outboxEvent.getEventId();

    try {
      // Este método solo retorna normalmente cuando Kafka responde con su ACK.
      this.inventoryEventProducer.publishAndWait(outboxEvent);

      // PUBLISHED significa que Kafka confirmó la publicación; no significa que
      // el servicio consumidor ya haya procesado el evento.
      this.outboxEventService.markAsPublished(eventId);
    } catch (RuntimeException exception) {
      // Ante timeout o error de Kafka no se marca PUBLISHED. El registro continúa
      // PENDING para que un ciclo posterior del scheduler vuelva a intentarlo.
      String errorMessage = this.limitErrorMessage(exception.getMessage());
      this.outboxEventService.registerFailedAttempt(eventId, errorMessage);

      String eventType = outboxEvent.getEventType();
      String correlationId = outboxEvent.getCorrelationId();
      String aggregateId = outboxEvent.getAggregateId();
      int i = outboxEvent.getAttempts() + 1;
      log.error("action=outbox_publish_failed eventType={} eventId={} correlationId={} orderId={} attempts={}", eventType, eventId, correlationId, aggregateId, i, exception);
    }
  }

  private String limitErrorMessage(String errorMessage) {
    if (errorMessage == null) {
      return "Unknown error";
    }
    return errorMessage.substring(0, Math.min(errorMessage.length(), 2000));
  }

}