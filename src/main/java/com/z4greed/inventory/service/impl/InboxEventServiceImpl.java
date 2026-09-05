package com.z4greed.inventory.service.impl;

import com.z4greed.inventory.entity.InboxEventEntity;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.mapper.InboxEventMapper;
import com.z4greed.inventory.repository.InboxEventRepository;
import com.z4greed.inventory.service.InboxEventService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InboxEventServiceImpl implements InboxEventService {
  private final InboxEventRepository inboxEventRepository;
  private final InboxEventMapper inboxEventMapper;

  public InboxEventServiceImpl(
      InboxEventRepository inboxEventRepository,
      InboxEventMapper inboxEventMapper
  ) {
    this.inboxEventRepository = inboxEventRepository;
    this.inboxEventMapper = inboxEventMapper;
  }

  @Override
  @Transactional(readOnly = true)
  public boolean wasAlreadyProcessed(String eventId) {
    return this.inboxEventRepository.existsById(eventId);
  }

  @Override
  @Transactional
  public void register(EventEnvelopeDto eventEnvelopeDto) {
    InboxEventEntity inboxEvent = this.inboxEventMapper.toEntity(eventEnvelopeDto);
    this.inboxEventRepository.save(inboxEvent);
  }

}