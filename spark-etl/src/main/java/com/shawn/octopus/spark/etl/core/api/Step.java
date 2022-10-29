package com.shawn.octopus.spark.etl.core.api;

import com.shawn.octopus.spark.etl.core.common.StepType;
import com.shawn.octopus.spark.etl.core.model.ETLContext;

public interface Step {

  StepType getStepType();

  String getName();

  void process(ETLContext context);

  StepConfiguration getConfig();
}
