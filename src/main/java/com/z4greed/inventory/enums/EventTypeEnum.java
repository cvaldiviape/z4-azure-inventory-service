package com.z4greed.inventory.enums;

import java.util.Arrays;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EventTypeEnum {
  RESERVE_STOCK("RESERVE_STOCK"),
  RELEASE_STOCK("RELEASE_STOCK"),
  STOCK_RESERVED("STOCK_RESERVED"),
  STOCK_NOT_AVAILABLE("STOCK_NOT_AVAILABLE"),
  STOCK_RELEASED("STOCK_RELEASED");

  private final String value;

  public static Optional<EventTypeEnum> fromValue(String value) {
    EventTypeEnum[] listEnumValues = values();

    return Arrays.stream(listEnumValues)
            .filter(eventTypeEnum -> {
              String valueEvent = eventTypeEnum.value;
              return valueEvent.equals(value);
            })
            .findFirst();
  }

}
