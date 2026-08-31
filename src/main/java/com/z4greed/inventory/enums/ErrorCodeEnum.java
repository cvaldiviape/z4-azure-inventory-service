package com.z4greed.inventory.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum {
  INVALID_EVENT("Invalid event"),
  INVALID_RESERVED_STOCK("Invalid reserved stock"),
  EVENT_PUBLISH_FAILED("Event publish failed");
  private final String message;
}
