package com.octopus.operators.engine.exception;

import org.slf4j.helpers.MessageFormatter;

public class CommonExceptionConstant {

  private static final String UNSUPPORTED_DATA_TYPE = "The data type {} is not supported";

  public static String unsupportedDataType(String dataType) {
    return MessageFormatter.arrayFormat(UNSUPPORTED_DATA_TYPE, new Object[] {dataType})
        .getMessage();
  }
}
