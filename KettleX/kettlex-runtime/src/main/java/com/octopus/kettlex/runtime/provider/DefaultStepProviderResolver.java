package com.octopus.kettlex.runtime.provider;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.provider.StepConfigStepCombination;
import com.octopus.kettlex.core.provider.StepConfigStepCombinationsBuilder;
import com.octopus.kettlex.core.provider.StepProviderResolver;

public class DefaultStepProviderResolver extends StepProviderResolver {

  @Override
  public StepConfigStepCombination getStepConfigStepCombination(String type) {
    return getBuilder().build().stream()
        .filter(stepConfigStepCombination -> stepConfigStepCombination.getType().equals(type))
        .findFirst()
        .orElseThrow(() -> new KettleXException(String.format("step [%s] not found. ", type)));
  }

  @Override
  public StepConfigStepCombinationsBuilder getBuilder() {
    return newStepConfigStepCombinationBuilder(resolveClassLoader(null));
  }

  private ClassLoader resolveClassLoader(ClassLoader classLoader) {
    return classLoader == null ? this.getClass().getClassLoader() : classLoader;
  }

  private StepConfigStepCombinationsBuilder newStepConfigStepCombinationBuilder(
      ClassLoader classLoader) {
    return new DefaultStepConfigStepCombinationsBuilder(classLoader)
        //        .addBuiltInStepConfigSteps()
        .addDiscoveredStepConfigSteps();
  }
}
