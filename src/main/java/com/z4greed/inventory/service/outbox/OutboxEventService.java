package com.z4greed.inventory.service.outbox;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.enums.ErrorCodeEnum;
import com.z4greed.inventory.enums.OutboxStatusEnum;
import com.z4greed.inventory.exception.CustomNonRetryableKafkaException;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.repository.OutboxEventRepository;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class OutboxEventService {
  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper mapper;

  public OutboxEventService(
      OutboxEventRepository outboxEventRepository,
      ObjectMapper mapper) {
    this.outboxEventRepository = outboxEventRepository;
    this.mapper = mapper;
  }

  // Esta inserción participa en la misma transacción de inventario. Si la reserva,
  // inbox_events o este INSERT falla, PostgreSQL revierte todas las operaciones.
  @Transactional
  public void enqueue(String topic, EventEnvelopeDto eventEnvelopeDto) {
    String payload = this.serialize(eventEnvelopeDto);
    OutboxEventEntity outboxEvent = OutboxEventEntity.builder()
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

    // Se mantiene fuera del catch de serialización para que Spring conserve la clase
    // original de cualquier error de base de datos y aplique su política de reintentos.
    this.outboxEventRepository.save(outboxEvent);
  }

  private String serialize(EventEnvelopeDto eventEnvelopeDto) {
    try {
      return this.mapper.writeValueAsString(eventEnvelopeDto);
    } catch (Exception exception) {
      throw new CustomNonRetryableKafkaException(
          ErrorCodeEnum.OUTBOX_SERIALIZATION_FAILED,
          exception);
    }
  }

  @Transactional(readOnly = true)
  public List<OutboxEventEntity> findPending(int batchSize) {
    return this.outboxEventRepository.findByStatusOrderByCreatedAtAsc(
        OutboxStatusEnum.PENDING,
        PageRequest.of(0, batchSize));
  }

  @Transactional
  public void markAsPublished(String eventId) {
    this.outboxEventRepository.findById(eventId).ifPresent(outboxEvent -> {
      outboxEvent.setStatus(OutboxStatusEnum.PUBLISHED);
      outboxEvent.setPublishedAt(LocalDateTime.now());
      outboxEvent.setLastError(null);
    });
  }

  @Transactional
  public void registerFailedAttempt(String eventId, String errorMessage) {
    this.outboxEventRepository.findById(eventId).ifPresent(outboxEvent -> {
      outboxEvent.setAttempts(outboxEvent.getAttempts() + 1);
      outboxEvent.setLastError(errorMessage);
    });
  }
}
