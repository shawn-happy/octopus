package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.api.Step;
import com.shawn.octopus.spark.etl.core.enums.SourceType;
import com.shawn.octopus.spark.etl.core.enums.StepType;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Source extends Step {

  SourceType getFormat();

  @Override
  default StepType getStepType() {
    return StepType.Source;
  }

  Dataset<Row> read(ETLContext context);
}
