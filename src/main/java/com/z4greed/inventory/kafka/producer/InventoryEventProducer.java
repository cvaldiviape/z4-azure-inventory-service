package com.z4greed.inventory.kafka.producer;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.exception.CustomRetryableKafkaException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InventoryEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final long sendTimeoutMilliseconds;

  public InventoryEventProducer(
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.outbox.send-timeout-milliseconds}") long sendTimeoutMilliseconds) {
    this.kafkaTemplate = kafkaTemplate;
    this.sendTimeoutMilliseconds = sendTimeoutMilliseconds;
  }

  // El ACK de Kafka es obligatorio antes de que el publicador cambie PENDING a PUBLISHED.
  public void publishAndWait(OutboxEventEntity outboxEvent) {
    try {
      var sendResult = this.kafkaTemplate
          .send(outboxEvent.getTopic(), outboxEvent.getEventKey(), outboxEvent.getPayload())
          .get(this.sendTimeoutMilliseconds, TimeUnit.MILLISECONDS);

      log.info(
          "action=event_published topic={} partition={} offset={} eventType={} eventId={} orderId={}",
          outboxEvent.getTopic(),
          sendResult.getRecordMetadata().partition(),
          sendResult.getRecordMetadata().offset(),
          outboxEvent.getEventType(),
          outboxEvent.getEventId(),
          outboxEvent.getAggregateId());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new CustomRetryableKafkaException("Kafka publication was interrupted", exception);
    } catch (Exception exception) {
      throw new CustomRetryableKafkaException("Kafka did not confirm the outbox event", exception);
    }
  }

}
