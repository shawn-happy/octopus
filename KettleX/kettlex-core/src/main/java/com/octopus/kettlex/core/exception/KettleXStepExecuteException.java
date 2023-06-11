package com.octopus.kettlex.core.exception;

public class KettleXStepExecuteException extends KettleXException {

  public KettleXStepExecuteException() {}

  public KettleXStepExecuteException(String message) {
    super(message);
  }

  public KettleXStepExecuteException(Throwable cause) {
    super(cause);
  }

  public KettleXStepExecuteException(String message, Throwable cause) {
    super(message, cause);
  }
}
