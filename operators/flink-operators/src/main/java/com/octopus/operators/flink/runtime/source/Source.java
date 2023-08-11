package com.octopus.operators.flink.runtime.source;

import com.octopus.operators.flink.declare.source.SourceDeclare;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface Source<P extends SourceDeclare<?>> {

  DataStream<Row> reader() throws Exception;
}
