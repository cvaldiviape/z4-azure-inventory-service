package com.z4greed.inventory.service.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventStrategyRegistry {
  private final Map<EventTypeEnum, InventoryEventStrategy> mapEventStrategies;

  public InventoryEventStrategyRegistry(
      List<InventoryEventStrategy> listEventStrategies
  ) {
    // Spring inyecta todos los beans que implementan InventoryEventStrategy.
    this.mapEventStrategies = new EnumMap<>(EventTypeEnum.class);

    listEventStrategies.forEach(eventStrategy -> {
      EventTypeEnum eventTypeEnum = eventStrategy.getEventType();
      this.mapEventStrategies.put(eventTypeEnum, eventStrategy);
    });
  }

  public InventoryEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }
}
