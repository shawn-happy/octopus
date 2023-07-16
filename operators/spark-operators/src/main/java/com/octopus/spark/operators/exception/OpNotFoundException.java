package com.octopus.spark.operators.exception;

public class OpNotFoundException extends SparkRuntimeException {

  private static final String ERROR_MESSAGE = "op not found!";

  public OpNotFoundException() {
    super(ERROR_MESSAGE);
  }

  public OpNotFoundException(String opType) {
    super(String.format("%s, op type: [%s]", ERROR_MESSAGE, opType));
  }

  public OpNotFoundException(String opType, Throwable cause) {
    super(String.format("%s, op type: [%s]", ERROR_MESSAGE, opType), cause);
  }

  public OpNotFoundException(Throwable cause) {
    super(ERROR_MESSAGE, cause);
  }
}
