package com.octopus.actus.common.exception;

public class ActusException extends RuntimeException {

  public ActusException() {
    super();
  }

  public ActusException(String message) {
    super(message);
  }

  public ActusException(String message, Throwable cause) {
    super(message, cause);
  }

  public ActusException(Throwable cause) {
    super(cause);
  }
}
