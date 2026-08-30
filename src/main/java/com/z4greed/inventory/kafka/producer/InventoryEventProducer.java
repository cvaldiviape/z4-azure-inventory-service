package com.z4greed.inventory.kafka.producer;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.exception.GreedException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
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
      this.kafkaTemplate.send(topic, eventEnvelopeDto.aggregateId(), eventJson);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}
