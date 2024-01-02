package com.octopus.operators.connector.flink.transform;

import com.octopus.operators.connector.flink.DataStreamTableInfo;
import com.octopus.operators.connector.flink.FlinkAbstractExecuteProcessor;
import com.octopus.operators.connector.flink.FlinkJobContext;
import com.octopus.operators.connector.flink.FlinkRuntimeEnvironment;
import com.octopus.operators.engine.connector.transform.Transform;
import com.octopus.operators.engine.connector.transform.sql.SqlTransformConfig;
import com.octopus.operators.engine.connector.transform.sql.SqlTransformConfig.SqlTransformOptions;
import com.octopus.operators.engine.exception.EngineException;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlTransform extends FlinkAbstractExecuteProcessor
    implements Transform<DataStreamTableInfo, FlinkRuntimeEnvironment> {

  private final SqlTransformConfig config;
  private final SqlTransformOptions options;

  public SqlTransform(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment,
      FlinkJobContext jobContext,
      SqlTransformConfig config) {
    super(flinkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public DataStreamTableInfo transform() {
    StreamTableEnvironment streamTableEnvironment =
        flinkRuntimeEnvironment.getStreamTableEnvironment();
    List<String> inputs = config.getInputs();
    boolean tableExists = jobContext.tableExists(inputs);
    if (!tableExists) {
      throw new EngineException("table not found");
    }
    Table table = streamTableEnvironment.sqlQuery(options.getSql());
    DataStream<Row> dataStream = streamTableEnvironment.toDataStream(table);
    jobContext.registerTable(config.getOutput(), dataStream);
    return DataStreamTableInfo.of(config.getOutput(), dataStream);
  }
}
