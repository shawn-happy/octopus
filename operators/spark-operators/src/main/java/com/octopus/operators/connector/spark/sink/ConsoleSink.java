package com.octopus.operators.connector.spark.sink;

import com.octopus.operators.connector.spark.DatasetTableInfo;
import com.octopus.operators.connector.spark.SparkAbstractExecuteProcessor;
import com.octopus.operators.connector.spark.SparkJobContext;
import com.octopus.operators.connector.spark.SparkRuntimeEnvironment;
import com.octopus.operators.engine.connector.sink.Sink;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig.ConsoleSinkOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ConsoleSink extends SparkAbstractExecuteProcessor
    implements Sink<DatasetTableInfo, SparkRuntimeEnvironment> {

  private final ConsoleSinkConfig config;
  private final ConsoleSinkOptions options;

  public ConsoleSink(
      SparkRuntimeEnvironment sparkRuntimeEnvironment,
      SparkJobContext jobContext,
      ConsoleSinkConfig config) {
    super(sparkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void output(DatasetTableInfo datasetTableInfo) {
    String input = config.getInput();
    Dataset<Row> dataset = jobContext.getDataset(input);
    dataset.show();
  }
}
