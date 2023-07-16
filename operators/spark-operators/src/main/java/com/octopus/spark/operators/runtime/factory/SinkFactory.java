package com.octopus.spark.operators.runtime.factory;

import com.octopus.spark.operators.declare.common.SinkType;
import com.octopus.spark.operators.declare.sink.CSVSinkDeclare;
import com.octopus.spark.operators.declare.sink.IcebergSinkDeclare;
import com.octopus.spark.operators.declare.sink.JDBCSinkDeclare;
import com.octopus.spark.operators.declare.sink.SinkDeclare;
import com.octopus.spark.operators.exception.SparkRuntimeException;
import com.octopus.spark.operators.runtime.step.sink.CSVSink;
import com.octopus.spark.operators.runtime.step.sink.IcebergSink;
import com.octopus.spark.operators.runtime.step.sink.JDBCSink;
import com.octopus.spark.operators.runtime.step.sink.Sink;

public class SinkFactory {

  public static Sink<? extends SinkDeclare<?>> createSink(SinkDeclare<?> sinkDeclare) {
    SinkType type = sinkDeclare.getType();
    Sink<? extends SinkDeclare<?>> sink = null;
    switch (type) {
      case csv:
        sink = new CSVSink((CSVSinkDeclare) sinkDeclare);
        break;
      case jdbc:
        sink = new JDBCSink((JDBCSinkDeclare) sinkDeclare);
        break;
      case iceberg:
        sink = new IcebergSink((IcebergSinkDeclare) sinkDeclare);
        break;
      default:
        throw new SparkRuntimeException("unsupported sink type: " + type);
    }
    return sink;
  }
}
