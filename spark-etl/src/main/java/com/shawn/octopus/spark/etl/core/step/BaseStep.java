package com.shawn.octopus.spark.etl.core.step;

public abstract class BaseStep implements Step {

  private final String name;
  private final StepConfiguration config;

  public BaseStep(String name, StepConfiguration config) {
    this.name = name;
    this.config = config;
  }

  @Override
  public StepConfiguration getConfig() {
    return config;
  }

  @Override
  public String getName() {
    return name;
  }
}
