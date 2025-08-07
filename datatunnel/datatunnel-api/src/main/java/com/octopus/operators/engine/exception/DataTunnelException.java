package com.octopus.operators.engine.exception;

public abstract class DataTunnelException extends RuntimeException {

  public DataTunnelException(String message) {
    super(message);
  }

  public DataTunnelException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataTunnelException(Throwable cause) {
    super(cause);
  }
}
