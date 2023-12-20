package com.octopus.operators.spark.runtime;

public abstract class SparkAbstractExecuteProcessor {

  protected SparkRuntimeEnvironment sparkRuntimeEnvironment;
  protected final SparkJobContext jobContext;

  protected SparkAbstractExecuteProcessor(
      SparkRuntimeEnvironment sparkRuntimeEnvironment, SparkJobContext jobContext) {
    this.sparkRuntimeEnvironment = sparkRuntimeEnvironment;
    this.jobContext = jobContext;
  }

  public void setRuntimeEnvironment(SparkRuntimeEnvironment sparkRuntimeEnvironment) {
    this.sparkRuntimeEnvironment = sparkRuntimeEnvironment;
  }
}
