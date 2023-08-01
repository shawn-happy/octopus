package com.octopus.operators.kettlex.runtime.executor.runner;

import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
import com.octopus.operators.kettlex.core.steps.Step;
import com.octopus.operators.kettlex.core.steps.config.StepConfigChannelCombination;

public abstract class AbstractRunner implements Runner {

  private Step<?> step;
  private ExecutionStatus status;
  private final StepConfigChannelCombination<?> combination;

  protected AbstractRunner(Step<?> step, StepConfigChannelCombination<?> combination) {
    this.step = step;
    this.combination = combination;
  }

  @Override
  public void mark(ExecutionStatus status) {
    combination.getStepContext().getCommunication().markStatus(status);
  }

  @Override
  public Communication getCommunication() {
    return combination.getStepContext().getCommunication();
  }

  @Override
  public void shutdown() {
    step.shutdown();
  }
}
