package com.z4greed.inventory.service.impl;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.OutboxStatusEnum;
import com.z4greed.inventory.exception.CustomNonRetryableKafkaException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.repository.OutboxEventRepository;
import com.z4greed.inventory.service.OutboxEventService;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class OutboxEventServiceImpl implements OutboxEventService {
  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper mapper;

  public OutboxEventServiceImpl(OutboxEventRepository outboxEventRepository, ObjectMapper mapper) {
    this.outboxEventRepository = outboxEventRepository;
    this.mapper = mapper;
  }

  @Override
  @Transactional
  public void enqueue(String topic, EventEnvelopeDto eventEnvelopeDto) {
    String payload = this.serialize(eventEnvelopeDto);

    OutboxEventEntity outboxEventEntity = OutboxEventEntity.builder()
        .eventId(eventEnvelopeDto.eventId())
        .aggregateId(eventEnvelopeDto.aggregateId())
        .correlationId(eventEnvelopeDto.correlationId())
        .eventType(eventEnvelopeDto.eventType())
        .topic(topic)
        .eventKey(eventEnvelopeDto.aggregateId())
        .payload(payload)
        .status(OutboxStatusEnum.PENDING)
        .attempts(0)
        .createdAt(LocalDateTime.now())
        .build();

    this.outboxEventRepository.save(outboxEventEntity);
  }

  private String serialize(EventEnvelopeDto eventEnvelopeDto) {
    try {
      return this.mapper.writeValueAsString(eventEnvelopeDto);
    } catch (Exception exception) {
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.OUTBOX_SERIALIZATION_FAILED, exception);
    }
  }

  @Override @Transactional(readOnly = true)
  public List<OutboxEventEntity> findPending(int batchSize) {
    PageRequest pageable = PageRequest.of(0, batchSize);
    return this.outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatusEnum.PENDING, pageable);
  }

  @Override @Transactional
  public void markAsPublished(String eventId) {
    Optional<OutboxEventEntity> outboxEventEntity = this.outboxEventRepository.findById(eventId);

    outboxEventEntity.ifPresent(outboxEvent -> {
      outboxEvent.setStatus(OutboxStatusEnum.PUBLISHED);
      outboxEvent.setPublishedAt(LocalDateTime.now());
      outboxEvent.setLastError(null);
    });
  }

  @Override@Transactional
  public void registerFailedAttempt(String eventId, String errorMessage) {
    Optional<OutboxEventEntity> outboxEventEntity = this.outboxEventRepository.findById(eventId);

    outboxEventEntity.ifPresent(outboxEvent -> {
      outboxEvent.setAttempts(outboxEvent.getAttempts() + 1);
      outboxEvent.setLastError(errorMessage);
    });
  }

}