package com.z4greed.inventory.enums;

import com.z4greed.shared.core.exception.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

// Los errores de eventos inválidos o de serialización del outbox ahora los lanza z4-shared-kafka.
@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum implements ErrorCode {
    INVALID_RESERVED_STOCK(HttpStatus.CONFLICT, "Invalid reserved stock");
    private final HttpStatus httpStatus;
    private final String message;
}
