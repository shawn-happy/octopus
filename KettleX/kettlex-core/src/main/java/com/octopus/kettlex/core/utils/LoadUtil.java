package com.octopus.kettlex.core.utils;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.core.provider.StepProviderResolver;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.config.StepConfig;

public class LoadUtil {

  private static final StepProviderResolver stepProviderResolver =
      StepProviderResolver.getInstance();

  private LoadUtil() {}

  public static Step<?> loadStep(String type) {
    StepConfigStepCombination stepConfigStepCombination =
        stepProviderResolver.getStepConfigStepCombination(type);
    Class<?> stepClass = stepConfigStepCombination.getStepClass();
    try {
      return (Step<?>) stepClass.getConstructor().newInstance();
    } catch (Exception e) {
      throw new KettleXException("load step error. ", e);
    }
  }

  public static StepConfig<?> loadStepConfig(String type) {
    StepConfigStepCombination stepConfigStepCombination =
        stepProviderResolver.getStepConfigStepCombination(type);
    Class<?> stepConfigClass = stepConfigStepCombination.getStepConfigClass();
    try {
      return (StepConfig<?>) stepConfigClass.getConstructor().newInstance();
    } catch (Exception e) {
      throw new KettleXException("load step error. ", e);
    }
  }
}
