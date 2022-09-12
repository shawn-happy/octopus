package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.Output;
import com.shawn.octopus.spark.etl.core.StepConfiguration;
import com.shawn.octopus.spark.etl.core.StepType;
import java.util.List;

public abstract class SourceConfiguration implements StepConfiguration {

  private List<Output> outputs;

  protected SourceConfiguration() {}

  @Override
  public StepType getStepType() {
    return StepType.Source;
  }

  public void setOutputs(List<Output> outputs) {
    this.outputs = outputs;
  }

  public List<Output> getOutputs() {
    return outputs;
  }
}
