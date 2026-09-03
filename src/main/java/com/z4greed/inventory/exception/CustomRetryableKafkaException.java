package com.z4greed.inventory.exception;

public class CustomRetryableKafkaException extends RuntimeException {

  public CustomRetryableKafkaException(String message, Throwable cause) {
    super(message, cause);
  }
}
