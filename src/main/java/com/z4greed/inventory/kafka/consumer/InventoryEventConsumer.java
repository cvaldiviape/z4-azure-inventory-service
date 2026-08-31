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

  // Permanece a la escucha de RESERVE_STOCK y RELEASE_STOCK publicados en "inventory-commands-topic".
  @KafkaListener(topics = "inventory-commands-topic")
  public void consumeInventoryRelatedEvents(String rawEvent) {
    this.inventoryService.process(rawEvent);
  }

}
