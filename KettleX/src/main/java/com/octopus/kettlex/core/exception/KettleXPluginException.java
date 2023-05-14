package com.octopus.kettlex.core.exception;

public class KettleXPluginException extends KettleXException {

  public KettleXPluginException() {
    super();
  }

  public KettleXPluginException(String message) {
    super(message);
  }

  public KettleXPluginException(Throwable cause) {
    super(cause);
  }

  public KettleXPluginException(String message, Throwable cause) {
    super(message, cause);
  }
}
