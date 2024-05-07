package com.octopus.operators.connector.flink;

public abstract class FlinkAbstractExecuteProcessor {

  protected final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
  protected final FlinkJobContext jobContext;

  protected FlinkAbstractExecuteProcessor(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment, FlinkJobContext jobContext) {
    this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    this.jobContext = jobContext;
  }
}
