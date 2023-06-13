package com.octopus.kettlex.runtime.provider;

import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.reader.rowgenerator.RowGenerator;
import com.octopus.kettlex.reader.rowgenerator.RowGeneratorConfig;
import com.octopus.kettlex.steps.ValueMapper;
import com.octopus.kettlex.steps.ValueMapperConfig;

public enum BuiltInSteps {
  //  LOG_MESSAGE(
  //      new StepConfigStepCombination(
  //          "log-message",
  //          LogMessage.class,
  //          LogMessageConfig.class,
  //          Thread.currentThread().getContextClassLoader())),

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
