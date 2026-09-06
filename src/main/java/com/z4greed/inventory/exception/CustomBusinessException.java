package com.z4greed.inventory.exception;

import com.z4greed.inventory.enums.ErrorCodeEnum;
import lombok.Getter;

@Getter
public class CustomBusinessException extends RuntimeException {
  private final ErrorCodeEnum errorCode;

  public CustomBusinessException(ErrorCodeEnum errorCode) {
    super(errorCode.getMessage());
    this.errorCode = errorCode;
  }
}
