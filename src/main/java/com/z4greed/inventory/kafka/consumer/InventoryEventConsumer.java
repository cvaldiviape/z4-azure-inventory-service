package com.z4greed.inventory.kafka.consumer;

import com.z4greed.inventory.service.inventory.InventoryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventConsumer {
  private final InventoryService inventoryService;

  public InventoryEventConsumer(InventoryService inventoryService) {
    this.inventoryService = inventoryService;
  }

  // Permanece a la escucha de eventos publicados en "orders-events-topic" e "inventory-events-topic".
  @KafkaListener(topics = {"orders-events-topic", "inventory-events-topic"})
  public void consumeInventoryRelatedEvents(String rawEvent) {
    this.inventoryService.process(rawEvent);
  }

}