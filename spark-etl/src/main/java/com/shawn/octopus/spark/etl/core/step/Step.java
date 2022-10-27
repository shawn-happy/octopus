package com.shawn.octopus.spark.etl.core.step;

public interface Step {

  StepType getStepType();

  String getName();

  void process(StepContext context);

  StepConfiguration getConfig();
}
