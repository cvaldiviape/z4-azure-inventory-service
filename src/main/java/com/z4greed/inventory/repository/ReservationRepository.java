package com.z4greed.inventory.repository;

import com.z4greed.inventory.entity.ReservationEntity;
import com.z4greed.inventory.enums.ReservationStatusEnum;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<ReservationEntity, Long> {
  List<ReservationEntity> findByOrderIdAndStatus(Long orderId, ReservationStatusEnum status);
}
