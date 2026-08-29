package com.z4greed.inventory.service.inventory.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.inventory.entity.ProcessedEventEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.exception.GreedException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.mapper.ProcessedEventMapper;
import com.z4greed.inventory.repository.ProcessedEventRepository;
import com.z4greed.inventory.service.inventory.InventoryService;
import com.z4greed.inventory.service.inventory.strategy.InventoryEventStrategy;
import com.z4greed.inventory.service.inventory.strategy.InventoryEventStrategyRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

@Service
@Transactional
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
    Boolean wasProcessed = this.wasProcessed(eventEnvelopeDto);

    if (wasProcessed) {
      return;
    }

    InventoryEventStrategy eventStrategy = this.findEventStrategy(eventEnvelopeDto);

    if (eventStrategy == null) {
      return;
    }

    eventStrategy.execute(eventEnvelopeDto);
    this.markAsProcessed(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
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
