package com.octopus.operators.flink.runtime;

public abstract class FlinkAbstractExecuteProcessor {

  protected FlinkRuntimeEnvironment flinkRuntimeEnvironment;
  protected final FlinkJobContext jobContext;

  protected FlinkAbstractExecuteProcessor(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment, FlinkJobContext jobContext) {
    this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    this.jobContext = jobContext;
  }

  public void setRuntimeEnvironment(FlinkRuntimeEnvironment flinkRuntimeEnvironment) {
    this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
  }
}
