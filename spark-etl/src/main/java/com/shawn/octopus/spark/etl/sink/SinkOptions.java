package com.shawn.octopus.spark.etl.sink;

import com.shawn.octopus.spark.etl.core.api.StepConfiguration;
import com.shawn.octopus.spark.etl.core.enums.WriteMode;

public interface SinkOptions extends StepConfiguration {
  String input();

  WriteMode getWriteMode();
}
