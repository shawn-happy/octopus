package com.octopus.operators.flink.runtime.source;

import com.octopus.operators.flink.declare.common.ColumnDesc;
import com.octopus.operators.flink.declare.source.CSVSourceDeclare;
import com.octopus.operators.flink.runtime.FlinkRuntimeEnvironment;
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

public class CSVFileSource implements Source<CSVSourceDeclare> {

  private final CSVSourceDeclare declare;
  private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;

  public CSVFileSource(FlinkRuntimeEnvironment flinkRuntimeEnvironment, CSVSourceDeclare declare) {
    this.declare = declare;
    this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
  }

  @Override
  public DataStream<Row> reader() throws Exception {
    StreamExecutionEnvironment executionEnvironment =
        flinkRuntimeEnvironment.getStreamExecutionEnvironment();
    var options = declare.getOptions();
    var paths =
        Arrays.stream(options.getPaths())
            .map(Path::new)
            .collect(Collectors.toUnmodifiableList())
            .toArray(new Path[] {});
    SerializableFunction<CsvMapper, CsvSchema> schemaGenerator =
        mapper -> mapper.schemaFor(Map.class).withHeader();

    List<ColumnDesc> schemas = options.getSchemas();

    var csvFormat =
        CsvReaderFormat.forSchema(CsvMapper::new, schemaGenerator, TypeInformation.of(Map.class));

    var fileSource = FileSource.forRecordStreamFormat(csvFormat, paths).build();

    return executionEnvironment
        .fromSource(fileSource, WatermarkStrategy.noWatermarks(), declare.getName())
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
                    .map(ColumnDesc::getName)
                    .collect(Collectors.toUnmodifiableList())
                    .toArray(new String[] {}),
                Types.STRING,
                Types.STRING,
                Types.STRING));
  }
}
