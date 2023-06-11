package com.octopus.kettlex.runtime.executor.runner;

import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.runtime.monitor.ExecutionStatus;

public abstract class AbstractRunner implements Runner {

  private Step<?> step;
  private ExecutionStatus status;

  protected AbstractRunner(Step<?> step) {
    this.step = step;
  }

  @Override
  public void destroy() {
    if (step != null) {
      step.destroy();
    }
  }

  @Override
  public void mark(ExecutionStatus status) {
    this.status = status;
  }

  @Override
  public void shutdown() {
    step.shutdown();
  }
}
