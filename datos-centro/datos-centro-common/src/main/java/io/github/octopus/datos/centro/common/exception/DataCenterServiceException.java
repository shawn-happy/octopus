package io.github.octopus.datos.centro.common.exception;

public class DataCenterServiceException extends RuntimeException{

  public DataCenterServiceException() {
  }

  public DataCenterServiceException(String message) {
    super(message);
  }

  public DataCenterServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataCenterServiceException(Throwable cause) {
    super(cause);
  }

  public DataCenterServiceException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
