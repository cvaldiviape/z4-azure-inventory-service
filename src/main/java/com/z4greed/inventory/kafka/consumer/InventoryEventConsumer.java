package com.z4greed.inventory.kafka.consumer;

import com.z4greed.inventory.command.InventoryCommandService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventConsumer {
  private final InventoryCommandService inventoryCommandService;

  public InventoryEventConsumer(InventoryCommandService inventoryCommandService) {
    this.inventoryCommandService = inventoryCommandService;
  }

  // Permanece a la escucha de los eventos de order-service publicados en "inventory-events-topic".
  @KafkaListener(topics = "inventory-commands-topic")
  public void consumeInventoryRelatedEvents(String rawEvent) {
    this.inventoryCommandService.process(rawEvent);
  }

}