package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.enums.Format;
import com.shawn.octopus.spark.etl.core.step.Step;
import com.shawn.octopus.spark.etl.core.step.StepContext;
import com.shawn.octopus.spark.etl.core.step.StepType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Source extends Step {

  Format getFormat();

  @Override
  default StepType getStepType() {
    return StepType.Source;
  }

  Dataset<Row> read(StepContext context);
}
