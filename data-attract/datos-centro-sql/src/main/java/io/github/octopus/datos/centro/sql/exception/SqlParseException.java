package io.github.octopus.datos.centro.sql.exception;

public class SqlParseException extends RuntimeException {
  public SqlParseException() {}

  public SqlParseException(String message) {
    super(message);
  }

  public SqlParseException(String message, Throwable cause) {
    super(message, cause);
  }

  public SqlParseException(Throwable cause) {
    super(cause);
  }

  public SqlParseException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
