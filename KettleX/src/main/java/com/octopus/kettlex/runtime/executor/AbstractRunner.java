package com.octopus.kettlex.runtime.executor;

import com.octopus.kettlex.core.steps.Step;

public abstract class AbstractRunner {

  private Step<?> step;

  protected AbstractRunner(Step<?> step) {
    this.step = step;
  }

  public void destroy() {
    if (step != null) {
      step.destroy();
    }
  }
}
