package io.github.shawn.octopus.fluxus.api.exception;

public class VerifyException extends DataWorkflowException {
  public VerifyException(String message) {
    super(message);
  }

  public VerifyException(String message, Throwable cause) {
    super(message, cause);
  }

  public VerifyException(Throwable cause) {
    super(cause);
  }
}
