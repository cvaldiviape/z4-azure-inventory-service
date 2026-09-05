package com.z4greed.inventory.entity;

import com.z4greed.inventory.enums.OutboxStatusEnum;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class OutboxEventEntity {
  @Id
  private String eventId;

  private String aggregateId;
  private String correlationId;
  private String eventType;
  private String topic;
  private String eventKey;

  @Column(columnDefinition = "TEXT")
  private String payload;

  @Enumerated(EnumType.STRING)
  private OutboxStatusEnum status;

  private Integer attempts;
  private LocalDateTime createdAt;
  private LocalDateTime publishedAt;

  @Column(columnDefinition = "TEXT")
  private String lastError;
}
