package com.z4greed.inventory.service;

import com.z4greed.inventory.entity.ReservationEntity;
import java.util.List;
import tools.jackson.databind.JsonNode;

public interface StockService {
  boolean tryReserve(
      Long orderId,
      JsonNode itemNode,
      List<ReservationEntity> listReservations,
      String eventId);

  List<ReservationEntity> findReservedByOrderId(Long orderId);

  void releaseAll(List<ReservationEntity> listReservations, String eventId);
}
