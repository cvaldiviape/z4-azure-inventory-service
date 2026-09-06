package com.z4greed.inventory.strategy;

public interface EventStrategyRegistry<TEventType, TContext> {
  EventStrategy<TEventType, TContext> find(TEventType eventType);
}
