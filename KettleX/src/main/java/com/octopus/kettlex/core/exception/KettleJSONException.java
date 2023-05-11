package com.octopus.kettlex.core.exception;

public class KettleJSONException extends KettleXException{

  public KettleJSONException() {
    super();
  }

  public KettleJSONException(String message) {
    super(message);
  }

  public KettleJSONException(Throwable cause) {
    super(cause);
  }

  public KettleJSONException(String message, Throwable cause) {
    super(message, cause);
  }
}
