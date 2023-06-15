package com.octopus.kettlex.runtime.executor.runner;

import com.octopus.kettlex.core.management.ExecutionStatus;

public interface Runner extends Runnable {

  void mark(ExecutionStatus status);

  default void markRun() {
    mark(ExecutionStatus.RUNNING);
  }

  default void markSuccess() {
    mark(ExecutionStatus.SUCCEEDED);
  }

  default void markFailed() {
    mark(ExecutionStatus.FAILED);
  }

  default void markKilled() {
    mark(ExecutionStatus.KILLED);
  }

  void shutdown();

  void destroy();
}
