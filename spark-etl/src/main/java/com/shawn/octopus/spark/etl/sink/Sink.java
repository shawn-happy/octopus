package com.shawn.octopus.spark.etl.sink;

import com.shawn.octopus.spark.etl.core.api.Step;
import com.shawn.octopus.spark.etl.core.common.StepType;
import com.shawn.octopus.spark.etl.core.enums.SinkType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Sink extends Step {

  SinkType getSinkType();

  @Override
  default StepType getStepType() {
    return StepType.Sink;
  }

  void write(Dataset<Row> df);
}
