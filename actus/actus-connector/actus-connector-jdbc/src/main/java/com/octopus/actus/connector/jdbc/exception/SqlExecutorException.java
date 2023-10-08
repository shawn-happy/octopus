package com.octopus.actus.connector.jdbc.exception;

import com.octopus.actus.common.exception.ActusException;

public class SqlExecutorException extends ActusException {

  public SqlExecutorException() {
    super();
  }

  public SqlExecutorException(String message) {
    super(message);
  }

  public SqlExecutorException(String message, Throwable cause) {
    super(message, cause);
  }

  public SqlExecutorException(Throwable cause) {
    super(cause);
  }
}
