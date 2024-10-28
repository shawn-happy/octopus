package io.github.octopus.datos.centro.sql.exception;

public class SqlFormatException extends RuntimeException {
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

  public SqlFormatException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
