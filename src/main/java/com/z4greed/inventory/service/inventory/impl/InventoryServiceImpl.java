package com.z4greed.inventory.service.inventory.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.entity.InboxEventEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.exception.CustomNonRetryableKafkaException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.mapper.InboxEventMapper;
import com.z4greed.inventory.repository.InboxEventRepository;
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
  private final InboxEventRepository inboxEventRepository;
  private final InboxEventMapper inboxEventMapper;
  private final InventoryEventStrategyRegistry eventStrategyRegistry;
  private final ObjectMapper mapper;

  public InventoryServiceImpl(
      InboxEventRepository inboxEventRepository,
      InboxEventMapper inboxEventMapper,
      InventoryEventStrategyRegistry eventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.inboxEventRepository = inboxEventRepository;
    this.inboxEventMapper = inboxEventMapper;
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

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private void processEvent(EventEnvelopeDto eventEnvelopeDto) {
    boolean wasAlreadyProcessed = this.wasAlreadyProcessed(eventEnvelopeDto);

    if (wasAlreadyProcessed) {
      log.info("action=event_ignored reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    InventoryEventStrategy eventStrategy = this.findEventStrategy(eventEnvelopeDto);

    if (eventStrategy == null) {
      log.info("action=event_ignored reason=unsupported_event_type eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }
    eventStrategy.execute(eventEnvelopeDto);
    
    this.registerInInbox(eventEnvelopeDto);
    log.info("action=event_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
  }

  private boolean wasAlreadyProcessed(EventEnvelopeDto eventEnvelopeDto) {
    String eventId = eventEnvelopeDto.eventId();
    return this.inboxEventRepository.existsById(eventId);
  }

  private InventoryEventStrategy findEventStrategy(EventEnvelopeDto eventEnvelopeDto) {
    String eventType = eventEnvelopeDto.eventType();
    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);

    return eventTypeEnum
        .map(this.eventStrategyRegistry::find)
        .orElse(null);
  }

  private void registerInInbox(EventEnvelopeDto eventEnvelopeDto) {
    InboxEventEntity inboxEvent = this.inboxEventMapper.toEntity(eventEnvelopeDto);
    this.inboxEventRepository.save(inboxEvent);
  }

}
