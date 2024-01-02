package com.octopus.operators.connector.flink.source;

import com.octopus.operators.connector.flink.DataStreamTableInfo;
import com.octopus.operators.connector.flink.FlinkAbstractExecuteProcessor;
import com.octopus.operators.connector.flink.FlinkDataTypeParser;
import com.octopus.operators.connector.flink.FlinkJobContext;
import com.octopus.operators.connector.flink.FlinkRuntimeEnvironment;
import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.engine.connector.source.csv.CSVSourceConfig;
import com.octopus.operators.engine.connector.source.csv.CSVSourceConfig.CSVSourceOptions;
import com.octopus.operators.engine.table.catalog.Column;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.SerializableFunction;

public class CSVSource extends FlinkAbstractExecuteProcessor
    implements Source<FlinkRuntimeEnvironment, CSVSourceConfig, DataStreamTableInfo> {

  private final CSVSourceConfig config;
  private final CSVSourceOptions options;

  public CSVSource(
      FlinkRuntimeEnvironment flinkRuntimeEnvironment,
      FlinkJobContext jobContext,
      CSVSourceConfig config) {
    super(flinkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public DataStreamTableInfo read() {
    StreamExecutionEnvironment executionEnvironment =
        flinkRuntimeEnvironment.getStreamExecutionEnvironment();
    var paths =
        Arrays.stream(options.getPaths())
            .map(Path::new)
            .collect(Collectors.toUnmodifiableList())
            .toArray(new Path[] {});
    SerializableFunction<CsvMapper, CsvSchema> schemaGenerator =
        mapper -> mapper.schemaFor(Map.class).withHeader();

    List<Column> schemas = options.getSchemas();

    var csvFormat =
        CsvReaderFormat.forSchema(CsvMapper::new, schemaGenerator, TypeInformation.of(Map.class));

    var fileSource = FileSource.forRecordStreamFormat(csvFormat, paths).build();
    DataStream<Row> ds =
        executionEnvironment
            .fromSource(fileSource, WatermarkStrategy.noWatermarks(), config.getName())
            .map(
                map -> {
                  List<Object> collection = Arrays.asList(map.values().toArray());
                  Row row = new Row(collection.size());
                  for (int i = 0; i < collection.size(); i++) {
                    row.setField(i, collection.get(i));
                  }
                  return row;
                })
            .returns(
                Types.ROW_NAMED(
                    schemas.stream()
                        .map(Column::getName)
                        .collect(Collectors.toUnmodifiableList())
                        .toArray(new String[] {}),
                    schemas.stream()
                        .map(Column::getRowDataType)
                        .map(FlinkDataTypeParser::parseTypeInformation)
                        .collect(Collectors.toUnmodifiableList())
                        .toArray(new TypeInformation[] {})));
    jobContext.registerTable(config.getOutput(), ds);
    return DataStreamTableInfo.of(config.getOutput(), ds);
  }
}
