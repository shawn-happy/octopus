package com.shawn.octopus.spark.etl.transform;

import com.shawn.octopus.spark.etl.core.step.Step;
import com.shawn.octopus.spark.etl.core.step.StepType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Transform extends Step {

  @Override
  default StepType getStepType() {
    return StepType.Transform;
  }

  Dataset<Row> processRow();
}
