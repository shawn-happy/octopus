package io.github.shawn.octopus.fluxus.api.exception;

public class DataWorkflowException extends RuntimeException {
  public DataWorkflowException() {}

  public DataWorkflowException(String message) {
    super(message);
  }

  public DataWorkflowException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataWorkflowException(Throwable cause) {
    super(cause);
  }
}
