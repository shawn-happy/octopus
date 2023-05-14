package com.octopus.kettlex.core.statistics;

public enum ExecutionState {
  SUBMITTING(10),
  INITIALIZING(20),
  WAITNG(30),
  RUNNING(40),
  KILLING(50),
  KILLED(60),
  FAILED(70),
  SUCCEEDED(80),
  ;

  private final int value;

  ExecutionState(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public boolean isFinished() {
    return this == KILLED || this == FAILED || this == SUCCEEDED;
  }

  public boolean isRunning() {
    return !isFinished();
  }
}
