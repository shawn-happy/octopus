package com.shawn.octopus.spark.etl.core.api;

import com.shawn.octopus.spark.etl.core.enums.StepType;
import com.shawn.octopus.spark.etl.core.model.ETLContext;

public interface Step {

  StepType getStepType();

  String getName();

  void process(ETLContext context);

  StepConfiguration getConfig();
}
