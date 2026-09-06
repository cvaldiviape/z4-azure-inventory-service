package com.z4greed.inventory.command.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.command.InventoryCommandService;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.exception.CustomNonRetryableKafkaException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.service.InboxEventService;
import com.z4greed.inventory.strategy.EventStrategy;
import com.z4greed.inventory.strategy.StockEventStrategyRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

@Service
@Transactional
@Slf4j
public class InventoryCommandServiceImpl implements InventoryCommandService {
  private final InboxEventService inboxEventService;
  private final StockEventStrategyRegistry stockEventStrategyRegistry;
  private final ObjectMapper mapper;

  public InventoryCommandServiceImpl(
      InboxEventService inboxEventService,
      StockEventStrategyRegistry stockEventStrategyRegistry,
      ObjectMapper mapper
  ) {
    this.inboxEventService = inboxEventService;
    this.stockEventStrategyRegistry = stockEventStrategyRegistry;
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
    String eventId = eventEnvelopeDto.eventId();
    boolean wasAlreadyProcessed = this.inboxEventService.wasAlreadyProcessed(eventId);

    if (wasAlreadyProcessed) {
      log.info("action=event_ignored reason=already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    EventStrategy<EventTypeEnum, EventEnvelopeDto> stockEventStrategy = this.findStockEventStrategy(eventEnvelopeDto);

    if (stockEventStrategy == null) {
      log.info("action=event_ignored reason=unsupported_event_type eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }
    stockEventStrategy.execute(eventEnvelopeDto);
    
    this.inboxEventService.register(eventEnvelopeDto, "inventory-commands-topic");
    log.info("action=event_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
  }

  private EventStrategy<EventTypeEnum, EventEnvelopeDto> findStockEventStrategy(EventEnvelopeDto eventEnvelopeDto) {
    String eventType = eventEnvelopeDto.eventType();
    Optional<EventTypeEnum> eventTypeEnum = EventTypeEnum.fromValue(eventType);

    return eventTypeEnum
        .map(this.stockEventStrategyRegistry::find)
        .orElse(null);
  }

}
