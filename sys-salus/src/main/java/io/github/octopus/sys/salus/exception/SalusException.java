package io.github.octopus.sys.salus.exception;

public class SalusException extends RuntimeException {

  public SalusException(String message) {
    super(message);
  }

  public SalusException(String message, Throwable cause) {
    super(message, cause);
  }
}
