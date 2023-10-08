package com.octopus.actus.connector.jdbc.exception;

import com.octopus.actus.common.exception.ActusException;

public class SqlFormatException extends ActusException {

  public SqlFormatException() {}

  public SqlFormatException(String message) {
    super(message);
  }

  public SqlFormatException(String message, Throwable cause) {
    super(message, cause);
  }

  public SqlFormatException(Throwable cause) {
    super(cause);
  }
}
