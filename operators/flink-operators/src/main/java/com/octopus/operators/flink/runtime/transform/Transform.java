package com.octopus.operators.flink.runtime.transform;

import com.octopus.operators.flink.declare.transform.TransformDeclare;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface Transform<P extends TransformDeclare<?>> {

  DataStream<Row> process(Map<String, DataStream<Row>> dataStreams) throws Exception;
}
