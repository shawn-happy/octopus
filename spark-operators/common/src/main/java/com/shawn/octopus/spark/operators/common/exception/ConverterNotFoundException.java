package com.shawn.octopus.spark.operators.common.exception;

public class ConverterNotFoundException extends SparkRuntimeException {

  private static final String ERROR_MESSAGE = "converter not found!";

  public ConverterNotFoundException() {
    super(ERROR_MESSAGE);
  }

  public ConverterNotFoundException(String converter) {
    super(String.format("%s, converter type: [%s]", ERROR_MESSAGE, converter));
  }

  public ConverterNotFoundException(String opType, Throwable cause) {
    super(String.format("%s, converter type: [%s]", ERROR_MESSAGE, opType), cause);
  }

  public ConverterNotFoundException(Throwable cause) {
    super(ERROR_MESSAGE, cause);
  }
}
