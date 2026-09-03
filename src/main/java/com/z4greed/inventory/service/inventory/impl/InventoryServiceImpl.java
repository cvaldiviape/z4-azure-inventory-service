package com.z4greed.inventory.service.inventory.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.entity.ProcessedEventEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.exception.CustomNonRetryableKafkaException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.mapper.ProcessedEventMapper;
import com.z4greed.inventory.repository.ProcessedEventRepository;
import com.z4greed.inventory.service.inventory.InventoryService;
import com.z4greed.inventory.service.inventory.strategy.InventoryEventStrategy;
import com.z4greed.inventory.service.inventory.strategy.InventoryEventStrategyRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

@Service
@Transactional
@Slf4j
public class InventoryServiceImpl implements InventoryService {
  private final ProcessedEventRepository processedEventRepository;
  private final ProcessedEventMapper processedEventMapper;
  private final InventoryEventStrategyRegistry eventStrategyRegistry;
  private final ObjectMapper mapper;

  public InventoryServiceImpl(
      ProcessedEventRepository processedEventRepository,
      ProcessedEventMapper processedEventMapper,
      InventoryEventStrategyRegistry eventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.processedEventRepository = processedEventRepository;
    this.processedEventMapper = processedEventMapper;
    this.eventStrategyRegistry = eventStrategyRegistry;
    this.mapper = mapper;
  }

  @Override
  public void process(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    log.info("action=event_received eventType={} eventId={} correlationId={} orderId={} producer={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), eventEnvelopeDto.producer());

    try {
      this.processEvent(eventEnvelopeDto);
    } catch (RuntimeException exception) {
      log.error("action=event_processing_failed eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
      throw exception;
    }
  }

  private void processEvent(EventEnvelopeDto eventEnvelopeDto) {
    Boolean wasProcessed = this.wasProcessed(eventEnvelopeDto);

    if (wasProcessed) {
      log.info("action=event_ignored reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    InventoryEventStrategy eventStrategy = this.findEventStrategy(eventEnvelopeDto);

    if (eventStrategy == null) {
      log.info("action=event_ignored reason=unsupported_event_type eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }
    eventStrategy.execute(eventEnvelopeDto);
    
    this.markAsProcessed(eventEnvelopeDto);
    log.info("action=event_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean wasProcessed(EventEnvelopeDto eventEnvelopeDto) {
    String eventId = eventEnvelopeDto.eventId();
    return this.processedEventRepository.existsById(eventId);
  }

  private InventoryEventStrategy findEventStrategy(EventEnvelopeDto eventEnvelopeDto) {
    String eventType = eventEnvelopeDto.eventType();
    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);

    return eventTypeEnum
        .map(this.eventStrategyRegistry::find)
        .orElse(null);
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

}
