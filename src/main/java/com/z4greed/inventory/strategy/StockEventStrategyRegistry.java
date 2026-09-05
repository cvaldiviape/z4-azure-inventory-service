package com.z4greed.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.strategy.impl.ReleaseStockEventStrategy;
import com.z4greed.inventory.strategy.impl.ReserveStockEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockEventStrategyRegistry {
  private final Map<EventTypeEnum, StockEventStrategy> mapStockEventStrategies;

  // Spring inyecta cada estrategia porque ambas están registradas como componentes.
  public StockEventStrategyRegistry(
      ReserveStockEventStrategy reserveStockEventStrategy,
      ReleaseStockEventStrategy releaseStockEventStrategy
  ) {
    // Cada tipo de evento queda asociado explícitamente con la estrategia que debe procesarlo.
    this.mapStockEventStrategies = Map.of(
        EventTypeEnum.RESERVE_STOCK, reserveStockEventStrategy,
        EventTypeEnum.RELEASE_STOCK, releaseStockEventStrategy
    );
  }

  public StockEventStrategy find(EventTypeEnum eventTypeEnum) {
    return this.mapStockEventStrategies.get(eventTypeEnum);
  }

}
