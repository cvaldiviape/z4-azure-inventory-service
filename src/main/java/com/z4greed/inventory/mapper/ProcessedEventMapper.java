package com.z4greed.inventory.mapper;

import com.z4greed.inventory.entity.ProcessedEventEntity;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import java.time.LocalDateTime;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, imports = LocalDateTime.class)
public interface ProcessedEventMapper {
  @Named("ProcessedEventMapper.toEntity")
  @Mapping(target = "processedAt", expression = "java(LocalDateTime.now())")
  ProcessedEventEntity toEntity(EventEnvelopeDto dto);
}
