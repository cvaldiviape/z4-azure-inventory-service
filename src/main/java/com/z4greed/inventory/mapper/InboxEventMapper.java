package com.z4greed.inventory.mapper;

import com.z4greed.inventory.entity.InboxEventEntity;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import java.time.LocalDateTime;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.Named;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, imports = LocalDateTime.class)
public interface InboxEventMapper {
  @Named("InboxEventMapper.toEntity")
  @Mapping(target = "processedAt", expression = "java(LocalDateTime.now())")
  InboxEventEntity toEntity(EventEnvelopeDto dto);
}
