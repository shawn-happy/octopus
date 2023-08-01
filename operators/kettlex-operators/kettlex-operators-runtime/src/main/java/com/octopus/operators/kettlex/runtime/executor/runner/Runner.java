package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;

public interface Runner extends Runnable {

  Communication getCommunication();

  default void markRun() {
    mark(ExecutionStatus.RUNNING);
  }

  default void markSuccess() {
    mark(ExecutionStatus.SUCCEEDED);
  }

  default void markFail(final Throwable throwable) {
    mark(ExecutionStatus.FAILED);
    getCommunication().setException(throwable);
  }

  void mark(ExecutionStatus status);

  void shutdown();
}
