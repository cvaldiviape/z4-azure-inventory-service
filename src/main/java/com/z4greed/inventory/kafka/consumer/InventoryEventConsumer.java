package com.z4greed.inventory.kafka.consumer;

import com.z4greed.inventory.service.InventoryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventConsumer {
  private final InventoryService inventoryService;

  public InventoryEventConsumer(InventoryService inventoryService) {
    this.inventoryService = inventoryService;
  }

  @KafkaListener(topics = {"orders.events", "inventory.events"})
  public void consume(String rawEvent) {
    this.inventoryService.process(rawEvent);
  }
}
