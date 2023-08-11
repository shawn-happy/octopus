package com.octopus.operators.flink.runtime.sink;

import com.octopus.operators.flink.declare.sink.SinkDeclare;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface Sink<P extends SinkDeclare<?>> {

  void writer(DataStream<Row> dataStream) throws Exception;
}
