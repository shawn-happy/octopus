package com.shawn.octopus.spark.etl.transform;

import com.shawn.octopus.spark.etl.core.api.Step;
import com.shawn.octopus.spark.etl.core.enums.StepType;
import com.shawn.octopus.spark.etl.core.enums.TransformType;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Transform extends Step {

  @Override
  default StepType getStepType() {
    return StepType.Transform;
  }

  TransformType getTransformType();

  Dataset<Row> trans(ETLContext context);
}
