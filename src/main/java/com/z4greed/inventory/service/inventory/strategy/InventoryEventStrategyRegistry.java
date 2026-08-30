package com.z4greed.inventory.service.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.service.inventory.strategy.impl.ReleaseStockEventStrategy;
import com.z4greed.inventory.service.inventory.strategy.impl.ReserveStockEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventStrategyRegistry {
  private final Map<EventTypeEnum, InventoryEventStrategy> mapEventStrategies;

  // Spring inyecta cada estrategia porque ambas están registradas como componentes.
  public InventoryEventStrategyRegistry(
      ReserveStockEventStrategy reserveStockEventStrategy,
      ReleaseStockEventStrategy releaseStockEventStrategy
  ) {
    // Cada tipo de evento queda asociado explícitamente con la estrategia que debe procesarlo.
    this.mapEventStrategies = Map.of(
        EventTypeEnum.ORDER_CREATED, reserveStockEventStrategy,
        EventTypeEnum.RELEASE_STOCK, releaseStockEventStrategy
    );
  }

  public InventoryEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapEventStrategies.get(eventTypeEnum);
  }

}