package com.shawn.octopus.spark.etl.transform;

import com.shawn.octopus.spark.etl.core.api.StepConfiguration;

public interface TransformOptions extends StepConfiguration {
  Integer getRePartition();

  String output();
}
