package io.github.shawn.octopus.fluxus.api.exception;

public class StepExecutionException extends DataWorkflowException {

  public StepExecutionException() {
    this("step error");
  }

  public StepExecutionException(String message) {
    super(message);
  }

  public StepExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public StepExecutionException(Throwable cause) {
    super(cause);
  }
}
