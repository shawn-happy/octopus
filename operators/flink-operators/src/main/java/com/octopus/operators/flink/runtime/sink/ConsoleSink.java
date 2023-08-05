package com.octopus.operators.flink.runtime.sink;

import com.octopus.operators.flink.declare.sink.ConsoleSinkDeclare;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConsoleSink implements Sink<ConsoleSinkDeclare> {

  @Override
  public void writer(StreamExecutionEnvironment executionEnvironment, DataStream<?> dataStream)
      throws Exception {
    dataStream.print();
  }
}
