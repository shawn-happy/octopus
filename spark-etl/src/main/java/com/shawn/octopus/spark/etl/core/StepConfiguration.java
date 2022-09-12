package com.shawn.octopus.spark.etl.core;

import java.util.Map;

public interface StepConfiguration {

  StepType getStepType();

  String getType();

  String getName();

  Map<String, String> getOptions();
}
