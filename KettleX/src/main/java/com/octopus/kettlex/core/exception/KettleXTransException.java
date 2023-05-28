package com.octopus.kettlex.core.exception;

public class KettleXTransException extends KettleXException {

  public KettleXTransException() {}

  public KettleXTransException(String message) {
    super(message);
  }

  public KettleXTransException(Throwable cause) {
    super(cause);
  }

  public KettleXTransException(String message, Throwable cause) {
    super(message, cause);
  }
}
