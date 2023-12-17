package com.octopus.operators.spark.runtime.source;

import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.spark.runtime.DatasetTableInfo;
import com.octopus.operators.spark.runtime.SparkAbstractExecuteProcessor;
import com.octopus.operators.spark.runtime.SparkJobContext;
import com.octopus.operators.spark.runtime.SparkRuntimeEnvironment;

public class FakeSource extends SparkAbstractExecuteProcessor
    implements Source<DatasetTableInfo, SparkRuntimeEnvironment> {

  public FakeSource(SparkRuntimeEnvironment sparkRuntimeEnvironment, SparkJobContext jobContext) {
    super(sparkRuntimeEnvironment, jobContext);
  }

  @Override
  public DatasetTableInfo read() {

    return null;
  }
}
