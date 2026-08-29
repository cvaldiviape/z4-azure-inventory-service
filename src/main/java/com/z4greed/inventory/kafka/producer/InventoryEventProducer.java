package com.z4greed.inventory.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.exception.GreedException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  public InventoryEventProducer(
      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
  }

  public void publish(EventEnvelopeDto eventEnvelopeDto) {
    try {
      String eventJson = this.objectMapper.writeValueAsString(eventEnvelopeDto);
      this.kafkaTemplate.send("inventory-events-topic", eventEnvelopeDto.aggregateId(), eventJson);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}