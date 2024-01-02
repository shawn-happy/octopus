package com.octopus.operators.connector.flink.sink;

import com.octopus.operators.connector.flink.DataStreamTableInfo;
import com.octopus.operators.connector.flink.FlinkAbstractExecuteProcessor;
import com.octopus.operators.connector.flink.FlinkJobContext;
import com.octopus.operators.connector.flink.FlinkRuntimeEnvironment;
import com.octopus.operators.engine.connector.sink.Sink;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig.ConsoleSinkOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class ConsoleSink extends FlinkAbstractExecuteProcessor
    implements Sink<DataStreamTableInfo, FlinkRuntimeEnvironment> {

  private final ConsoleSinkConfig config;
  private final ConsoleSinkOptions options;

  public ConsoleSink(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment,
      FlinkJobContext jobContext,
      ConsoleSinkConfig config) {
    super(flinkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public void output(DataStreamTableInfo dataStreamTableInfo) {
    DataStream<Row> dataStream = jobContext.getDataStream(config.getInput());
    dataStream.print();
  }
}
