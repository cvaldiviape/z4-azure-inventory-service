package com.z4greed.inventory.repository;

import com.z4greed.inventory.entity.OutboxEventEntity;
import com.z4greed.inventory.enums.OutboxStatusEnum;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {
  List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(
      OutboxStatusEnum status,
      Pageable pageable);
}
