package com.octopus.operators.flink.runtime.source;

import com.octopus.operators.flink.declare.source.SourceDeclare;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Source<P extends SourceDeclare<?>> {

  DataStream<?> reader(StreamExecutionEnvironment executionEnvironment) throws Exception;
}
