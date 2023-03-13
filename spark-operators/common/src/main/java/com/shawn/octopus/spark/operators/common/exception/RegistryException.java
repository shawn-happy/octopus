package com.shawn.octopus.spark.operators.common.exception;

public class RegistryException extends SparkRuntimeException {

  public RegistryException() {}

  public RegistryException(String message) {
    super(message);
  }

  public RegistryException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryException(Throwable cause) {
    super(cause);
  }
}
