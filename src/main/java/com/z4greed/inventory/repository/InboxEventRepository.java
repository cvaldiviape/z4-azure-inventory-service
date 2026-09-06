package com.z4greed.inventory.repository;

import com.z4greed.inventory.entity.InboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxEventRepository extends JpaRepository<InboxEventEntity, String> {}
