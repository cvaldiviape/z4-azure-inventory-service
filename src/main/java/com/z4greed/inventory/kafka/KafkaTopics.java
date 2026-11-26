package com.z4greed.inventory.kafka;

// Nombres de los topics que usa Inventory Service. Centralizarlos evita literales repetidos y errores de tipeo.
public final class KafkaTopics {
    public static final String INVENTORY_RESERVATION_REQUESTS = "inventory-reservation-requests-topic";
    public static final String INVENTORY_RESERVATION_RESULTS = "inventory-reservation-results-topic";

    private KafkaTopics() {
    }
}
