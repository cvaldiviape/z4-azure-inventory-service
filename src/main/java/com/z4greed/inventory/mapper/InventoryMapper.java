package com.z4greed.inventory.mapper;

import com.z4greed.inventory.dto.ReservationCreateDto;
import com.z4greed.inventory.entity.ReservationEntity;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface InventoryMapper {
  @Named("InventoryMapper.toEntity")
  @Mapping(target = "id", ignore = true)
  ReservationEntity toEntity(ReservationCreateDto dto);
}
