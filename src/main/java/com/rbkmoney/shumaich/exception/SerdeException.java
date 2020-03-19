package com.rbkmoney.shumaich.exception;

public class SerdeException extends RuntimeException {
    public SerdeException() {
    }

    public SerdeException(String message) {
        super(message);
    }

    public SerdeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerdeException(Throwable cause) {
        super(cause);
    }
}
