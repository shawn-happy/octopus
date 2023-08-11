package com.octopus.operators.flink.runtime.sink;

import com.octopus.operators.flink.declare.sink.ConsoleSinkDeclare;
import com.octopus.operators.flink.runtime.FlinkRuntimeEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class ConsoleSink implements Sink<ConsoleSinkDeclare> {

  private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
  private final ConsoleSinkDeclare sinkDeclare;

  public ConsoleSink(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment, ConsoleSinkDeclare sinkDeclare) {
    this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    this.sinkDeclare = sinkDeclare;
  }

  @Override
  public void writer(DataStream<Row> dataStream) throws Exception {
    dataStream.print();
  }
}
