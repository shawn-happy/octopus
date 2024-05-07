package com.octopus.operators.engine.config;

public class CheckResult {

  private static final CheckResult SUCCESS = new CheckResult(true, null);

  private final boolean success;
  private final String message;

  private CheckResult(boolean success, String message) {
    this.success = success;
    this.message = message;
  }

  public static CheckResult ok() {
    return SUCCESS;
  }

  public static CheckResult error(String message) {
    return new CheckResult(false, message);
  }

  public boolean isSuccess() {
    return success;
  }

  public String getMessage() {
    return message;
  }
}
