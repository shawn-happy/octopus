package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.api.StepConfiguration;

public interface SourceOptions extends StepConfiguration {
  Integer getRePartition();

  String output();
}
