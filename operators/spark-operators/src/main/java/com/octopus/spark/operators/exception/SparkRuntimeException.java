package com.octopus.spark.operators.exception;

public class SparkRuntimeException extends RuntimeException {

  public SparkRuntimeException() {}

  public SparkRuntimeException(String message) {
    super(message);
  }

  public SparkRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public SparkRuntimeException(Throwable cause) {
    super(cause);
  }
}
