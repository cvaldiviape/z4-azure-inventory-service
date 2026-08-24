package com.z4greed.inventory.enums;

import java.util.Arrays;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EventTypeEnum {
  ORDER_CREATED("ORDER_CREATED"),
  RELEASE_STOCK("RELEASE_STOCK"),
  STOCK_RESERVED("STOCK_RESERVED"),
  STOCK_NOT_AVAILABLE("STOCK_NOT_AVAILABLE"),
  STOCK_RELEASED("STOCK_RELEASED");

  private final String value;

  public static Optional<EventTypeEnum> fromValue(String value) {
    return Arrays.stream(values()).filter(eventTypeEnum -> eventTypeEnum.value.equals(value)).findFirst();
  }
}
