package com.z4greed.inventory.strategy;

import com.z4greed.inventory.enums.EventTypeEnum;
import com.z4greed.inventory.kafka.event.EventEnvelopeDto;
import com.z4greed.inventory.strategy.impl.ReleaseStockEventStrategy;
import com.z4greed.inventory.strategy.impl.ReserveStockEventStrategy;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockEventStrategyRegistry implements EventStrategyRegistry<EventTypeEnum, EventEnvelopeDto> {
  private final Map<EventTypeEnum, EventStrategy<EventTypeEnum, EventEnvelopeDto>> mapStockEventStrategies;

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
  public EventStrategy<EventTypeEnum, EventEnvelopeDto> find(EventTypeEnum eventTypeEnum) {
    return this.mapStockEventStrategies.get(eventTypeEnum);
  }

}