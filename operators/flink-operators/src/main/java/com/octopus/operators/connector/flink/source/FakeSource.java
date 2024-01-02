package com.octopus.operators.connector.flink.source;

import com.octopus.operators.connector.flink.DataStreamTableInfo;
import com.octopus.operators.connector.flink.FlinkAbstractExecuteProcessor;
import com.octopus.operators.connector.flink.FlinkDataTypeParser;
import com.octopus.operators.connector.flink.FlinkJobContext;
import com.octopus.operators.connector.flink.FlinkRuntimeEnvironment;
import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.engine.connector.source.fake.FakeDataGenerator;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.table.type.RowDataTypeParse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class FakeSource extends FlinkAbstractExecuteProcessor
    implements Source<FlinkRuntimeEnvironment, FakeSourceConfig, DataStreamTableInfo> {

  private final FakeSourceConfig config;
  private final FakeSourceOptions options;

  public FakeSource(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment,
      FlinkJobContext jobContext,
      FakeSourceConfig config) {
    super(flinkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public DataStreamTableInfo read() {
    StreamExecutionEnvironment executionEnvironment = jobContext.getExecutionEnvironment();
    FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(options);
    List<Row> rows = new ArrayList<>(options.getRowNum());
    for (int i = 0; i < options.getRowNum(); i++) {
      Object[] values = fakeDataGenerator.random();
      rows.add(Row.of(values));
    }
    TypeInformation<?>[] typeInformations =
        Arrays.stream(options.getFieldTypes())
            .map(RowDataTypeParse::parseDataType)
            .map(FlinkDataTypeParser::parseTypeInformation)
            .collect(Collectors.toUnmodifiableList())
            .toArray(new TypeInformation<?>[] {});
    DataStream<Row> result =
        executionEnvironment
            .fromCollection(rows)
            .returns(Types.ROW_NAMED(options.getFieldNames(), typeInformations))
            .name(config.getOutput());
    jobContext.registerTable(config.getOutput(), result);
    return DataStreamTableInfo.of(config.getOutput(), result);
  }
}
