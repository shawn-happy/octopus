package com.octopus.operators.connector.spark;

public abstract class SparkAbstractExecuteProcessor {

  protected final SparkRuntimeEnvironment sparkRuntimeEnvironment;
  protected final SparkJobContext jobContext;

  protected SparkAbstractExecuteProcessor(
      SparkRuntimeEnvironment sparkRuntimeEnvironment, SparkJobContext jobContext) {
    this.sparkRuntimeEnvironment = sparkRuntimeEnvironment;
    this.jobContext = jobContext;
  }
}
