package com.z4greed.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.IntegrationEventDto;
import com.z4greed.inventory.strategy.impl.ReleaseStockEventStrategy;
import com.z4greed.inventory.strategy.impl.ReserveStockEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockEventStrategyRegistry implements EventStrategyRegistry<EventTypeEnum, IntegrationEventDto> {
  private final Map<EventTypeEnum, EventStrategy<EventTypeEnum, IntegrationEventDto>> mapStockEventStrategies;

  // Spring inyecta cada estrategia porque ambas están registradas como componentes.
  public StockEventStrategyRegistry(
      ReserveStockEventStrategy reserveStockEventStrategy,
      ReleaseStockEventStrategy releaseStockEventStrategy
  ) {
    this.mapStockEventStrategies = Map.of(
        EventTypeEnum.RESERVE_STOCK, reserveStockEventStrategy,
        EventTypeEnum.RELEASE_STOCK, releaseStockEventStrategy
    );
  }

  @Override
  public EventStrategy<EventTypeEnum, IntegrationEventDto> find(EventTypeEnum eventTypeEnum) {
    return this.mapStockEventStrategies.get(eventTypeEnum);
  }

}
