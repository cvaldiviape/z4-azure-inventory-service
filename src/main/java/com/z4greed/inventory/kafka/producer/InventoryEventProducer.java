package com.z4greed.inventory.kafka.producer;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.exception.CustomRetryableKafkaException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
      String topic = outboxEvent.getTopic();
      String eventKey = outboxEvent.getEventKey();
      String payload = outboxEvent.getPayload();

      // send() inicia la publicación de manera asíncrona: devuelve inmediatamente un Future
      // que representa una operación todavía pendiente, no una confirmación del consumidor.
      CompletableFuture<SendResult<String, String>> send = this.kafkaTemplate.send(topic, eventKey, payload);

      // get() espera hasta que Kafka confirme la escritura (ACK) o venza el timeout.
      // Si retorna un SendResult, Kafka asignó una partición y un offset al mensaje.
      // Si Kafka rechaza el envío o no responde, get() lanza una excepción.
      SendResult<String, String> sendResult = send.get(this.sendTimeoutMilliseconds, TimeUnit.MILLISECONDS);

      log.info(
          "action=event_published topic={} partition={} offset={} eventType={} eventId={} orderId={}",
          topic,
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
