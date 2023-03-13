package com.shawn.octopus.spark.operators.common.exception;

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
