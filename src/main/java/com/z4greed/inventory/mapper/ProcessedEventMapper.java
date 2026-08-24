package com.z4greed.inventory.mapper;

import com.z4greed.inventory.entity.ProcessedEventEntity;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import java.time.Instant;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, imports = Instant.class)
public interface ProcessedEventMapper {
  @Named("ProcessedEventMapper.toEntity")
  @Mapping(target = "processedAt", expression = "java(Instant.now())")
  ProcessedEventEntity toEntity(EventEnvelopeDto dto);
}
