package io.github.shawn.octopus.fluxus.api.exception;

public class PipelineException extends DataWorkflowException {
  public PipelineException() {
    super();
  }

  public PipelineException(String message) {
    super(message);
  }

  public PipelineException(String message, Throwable cause) {
    super(message, cause);
  }

  public PipelineException(Throwable cause) {
    super(cause);
  }
}
