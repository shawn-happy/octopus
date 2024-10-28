package io.github.octopus.datos.centro.sql.exception;

public class SqlExecutorException extends RuntimeException {

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

  protected SqlExecutorException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
