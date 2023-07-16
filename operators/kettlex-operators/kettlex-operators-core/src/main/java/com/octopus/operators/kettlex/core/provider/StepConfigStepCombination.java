package com.octopus.operators.kettlex.core.provider;

public class StepConfigStepCombination {
  private final String type;
  private final Class<?> stepClass;
  private final Class<?> stepConfigClass;
  private final ClassLoader classLoader;

  public StepConfigStepCombination(
      String type, Class<?> stepClass, Class<?> stepConfigClass, ClassLoader classLoader) {
    this.type = type;
    this.stepClass = stepClass;
    this.stepConfigClass = stepConfigClass;
    this.classLoader = classLoader;
  }

  public String getType() {
    return type;
  }

  public Class<?> getStepClass() {
    return stepClass;
  }

  public Class<?> getStepConfigClass() {
    return stepConfigClass;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }
}
