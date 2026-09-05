package com.z4greed.inventory.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum {
  INVALID_EVENT("Invalid event"),
  INVALID_RESERVED_STOCK("Invalid reserved stock"),
  OUTBOX_SERIALIZATION_FAILED("Outbox event serialization failed");
  private final String message;
}
