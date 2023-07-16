package com.octopus.operators.kettlex.runtime.provider;

import com.octopus.operators.kettlex.builtin.logmessage.LogMessage;
import com.octopus.operators.kettlex.builtin.logmessage.LogMessageConfig;
import com.octopus.operators.kettlex.builtin.rowgenerator.RowGenerator;
import com.octopus.operators.kettlex.builtin.rowgenerator.RowGeneratorConfig;
import com.octopus.operators.kettlex.builtin.valuemapper.ValueMapper;
import com.octopus.operators.kettlex.builtin.valuemapper.ValueMapperConfig;
import com.octopus.operators.kettlex.core.provider.StepConfigStepCombination;

public enum BuiltInSteps {
  LOG_MESSAGE(
      new StepConfigStepCombination(
          "log-message",
          LogMessage.class,
          LogMessageConfig.class,
          Thread.currentThread().getContextClassLoader())),

  ROW_GENERATOR(
      new StepConfigStepCombination(
          "row-generator",
          RowGenerator.class,
          RowGeneratorConfig.class,
          Thread.currentThread().getContextClassLoader())),
  VALUE_MAPPER(
      new StepConfigStepCombination(
          "value-mapper",
          ValueMapper.class,
          ValueMapperConfig.class,
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
