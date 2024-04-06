package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.exception;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;

public class DorisConnectorException extends DataWorkflowException {
  public DorisConnectorException(String message) {
    super(message);
  }

  public DorisConnectorException(String message, Throwable cause) {
    super(message, cause);
  }

  public DorisConnectorException(Throwable cause) {
    super(cause);
  }
}
