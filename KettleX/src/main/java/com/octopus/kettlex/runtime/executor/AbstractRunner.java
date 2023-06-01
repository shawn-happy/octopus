package com.octopus.kettlex.runtime.executor;

import com.octopus.kettlex.model.StepConfig;

public abstract class AbstractRunner {

  private StepConfig<?> stepConfig;

  protected AbstractRunner(StepConfig<?> stepConfig) {}
}
