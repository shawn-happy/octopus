package io.github.shawn.octopus.fluxus.executor.exception;

public class DataWorkflowWebException extends RuntimeException {
  public DataWorkflowWebException() {
    super();
  }

  public DataWorkflowWebException(String message) {
    super(message);
  }

  public DataWorkflowWebException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataWorkflowWebException(Throwable cause) {
    super(cause);
  }

  protected DataWorkflowWebException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
