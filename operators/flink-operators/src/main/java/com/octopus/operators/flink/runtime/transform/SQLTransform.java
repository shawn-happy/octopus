package com.octopus.operators.flink.runtime.transform;

import com.octopus.operators.flink.declare.transform.SQLTransformDeclare;
import com.octopus.operators.flink.runtime.FlinkRuntimeEnvironment;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SQLTransform implements Transform<SQLTransformDeclare> {

  private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
  private final SQLTransformDeclare declare;

  public SQLTransform(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment, SQLTransformDeclare declare) {
    this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    this.declare = declare;
  }

  @Override
  public DataStream<Row> process(Map<String, DataStream<Row>> dataStreams) throws Exception {
    StreamTableEnvironment tableEnvironment = flinkRuntimeEnvironment.getStreamTableEnvironment();

    for (Map.Entry<String, DataStream<Row>> entry : dataStreams.entrySet()) {
      tableEnvironment.createTemporaryView(entry.getKey(), entry.getValue());
    }

    Table table = tableEnvironment.sqlQuery(declare.getOptions().getSql());
    return tableEnvironment.toChangelogStream(table);
  }
}
