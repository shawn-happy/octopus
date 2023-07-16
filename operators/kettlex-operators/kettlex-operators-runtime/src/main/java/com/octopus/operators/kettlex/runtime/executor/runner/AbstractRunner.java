package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.core.steps.Step;
import lombok.Getter;

public abstract class AbstractRunner implements Runner {

  private Step<?> step;
  private ExecutionStatus status;
  @Getter private final Communication communication;

  protected AbstractRunner(Step<?> step) {
    this.step = step;
    this.communication = new Communication();
  }

  @Override
  public void destroy() {
    if (step != null) {
      step.destroy();
    }
  }

  @Override
  public void mark(ExecutionStatus status) {
    communication.markStatus(status);
  }

  @Override
  public void shutdown() {
    step.shutdown();
  }
}
