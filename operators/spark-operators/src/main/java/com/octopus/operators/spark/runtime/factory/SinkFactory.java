package com.octopus.operators.spark.runtime.factory;

import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.sink.CSVSinkDeclare;
import com.octopus.operators.spark.declare.sink.IcebergSinkDeclare;
import com.octopus.operators.spark.declare.sink.JDBCSinkDeclare;
import com.octopus.operators.spark.declare.sink.SinkDeclare;
import com.octopus.operators.spark.exception.SparkRuntimeException;
import com.octopus.operators.spark.runtime.step.sink.CSVSink;
import com.octopus.operators.spark.runtime.step.sink.IcebergSink;
import com.octopus.operators.spark.runtime.step.sink.JDBCSink;
import com.octopus.operators.spark.runtime.step.sink.Sink;

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
