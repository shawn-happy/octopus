package com.octopus.kettlex.runtime.provider;

import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.steps.LogMessage;
import com.octopus.kettlex.steps.LogMessageConfig;

public enum BuiltInSteps {
  LOG_MESSAGE(
      new StepConfigStepCombination(
          "LOG-MESSAGE",
          LogMessage.class,
          LogMessageConfig.class,
          Thread.currentThread().getContextClassLoader())),
  ;

  private final StepConfigStepCombination stepConfigStepCombination;

  BuiltInSteps(StepConfigStepCombination stepConfigStepCombination) {
    this.stepConfigStepCombination = stepConfigStepCombination;
  }

  public StepConfigStepCombination getStepConfigStepCombination() {
    return stepConfigStepCombination;
  }
}
