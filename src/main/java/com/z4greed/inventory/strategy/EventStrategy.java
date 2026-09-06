package com.z4greed.inventory.strategy;

public interface EventStrategy<TEventType, TContext> {
  TEventType getEventType();
  void execute(TContext context);
}
