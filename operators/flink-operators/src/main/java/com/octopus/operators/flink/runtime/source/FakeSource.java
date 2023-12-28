package com.octopus.operators.flink.runtime.source;

import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.engine.connector.source.fake.FakeDataGenerator;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.flink.runtime.DataStreamTableInfo;
import com.octopus.operators.flink.runtime.FlinkAbstractExecuteProcessor;
import com.octopus.operators.flink.runtime.FlinkJobContext;
import com.octopus.operators.flink.runtime.FlinkRuntimeEnvironment;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class FakeSource extends FlinkAbstractExecuteProcessor
    implements Source<FlinkRuntimeEnvironment, FakeSourceConfig, DataStreamTableInfo> {

  private FakeSourceConfig config;

  public FakeSource(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment,
      FlinkJobContext jobContext,
      FakeSourceConfig config) {
    super(flinkRuntimeEnvironment, jobContext);
    this.config = config;
  }

  @Override
  public DataStreamTableInfo read() {
    FakeSourceOptions options = config.getOptions();
    StreamExecutionEnvironment executionEnvironment = jobContext.getExecutionEnvironment();
    FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(options);
    List<Row> rows = new ArrayList<>(options.getRowNum());
    for (int i = 0; i < options.getRowNum(); i++) {
      Object[] values = fakeDataGenerator.random();
      rows.add(Row.of(values));
    }
    executionEnvironment
        .fromCollection(rows)
        .returns(Types.ROW_NAMED(options.getFieldNames(), null));
    return null;
  }

  @Override
  public void setConfig(FakeSourceConfig config) {
    this.config = config;
  }
}
