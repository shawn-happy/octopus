package com.shawn.octopus.spark.etl.core.model;

import com.shawn.octopus.spark.etl.core.api.Step;
import com.shawn.octopus.spark.etl.core.api.StepConfiguration;

public abstract class BaseStep extends DefaultOperation implements Step {

  private final String name;
  private final StepConfiguration config;

  public BaseStep(String name, StepConfiguration config) {
    super(name);
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
