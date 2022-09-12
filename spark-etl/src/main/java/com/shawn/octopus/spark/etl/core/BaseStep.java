package com.shawn.octopus.spark.etl.core;

public abstract class BaseStep implements Step {

  private final StepContext context;

  private final StepConfiguration config;

  public BaseStep(StepContext context, StepConfiguration config) {
    this.context = context;
    this.config = config;
  }

  public StepContext getContext() {
    return context;
  }

  public StepConfiguration getConfig() {
    return config;
  }
}
