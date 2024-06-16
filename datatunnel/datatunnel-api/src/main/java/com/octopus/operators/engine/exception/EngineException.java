package com.octopus.operators.engine.exception;

public class EngineException extends RuntimeException {

  public EngineException(String message) {
    super(message);
  }

  public EngineException(String message, Throwable cause) {
    super(message, cause);
  }

  public EngineException(Throwable cause) {
    super(cause);
  }
}
