package com.z4greed.inventory.kafka.producer;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.exception.CustomNonRetryableKafkaException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InventoryEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper mapper;

  public InventoryEventProducer(
      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.mapper = mapper;
  }

  public void publish(String topic, EventEnvelopeDto eventEnvelopeDto) {
    try {
      String eventJson = this.mapper.writeValueAsString(eventEnvelopeDto);
      this.kafkaTemplate.send(topic, eventEnvelopeDto.aggregateId(), eventJson).whenComplete((sendResult, exception) -> {
        if (exception != null) {
          log.error("action=event_publish_failed topic={} eventType={} eventId={} correlationId={} orderId={}", topic, eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception);
          return;
        }

        log.info("action=event_published topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}", topic, sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset(), eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      });
    } catch (Exception exception) {
      log.error("action=event_serialization_failed topic={} eventType={} eventId={} correlationId={} orderId={}", topic, eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}
