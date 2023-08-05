package com.octopus.operators.flink.runtime.source;

import com.octopus.operators.flink.declare.common.SourceType;
import com.octopus.operators.flink.declare.source.FileSourceDeclare;
import com.octopus.operators.flink.declare.source.FileSourceDeclare.FileSourceOptions;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.jackson.JacksonMapperFactory;

public class LocalFileSource implements Source<FileSourceDeclare<FileSourceOptions>> {

  private final FileSourceDeclare<?> declare;

  public LocalFileSource(FileSourceDeclare<?> declare) {
    this.declare = declare;
  }

  @Override
  public DataStream<?> reader(StreamExecutionEnvironment executionEnvironment) throws Exception {
    SourceType type = declare.getType();
    FileSourceOptions options = declare.getOptions();
    switch (type) {
      case csv:
        FileSource<?> fileSource =
            FileSource.forRecordStreamFormat(
                    CsvReaderFormat.forSchema(
                        JacksonMapperFactory.createCsvMapper().schemaWithHeader(),
                        TypeInformation.of(Map.class)),
                    Arrays.stream(options.getPaths())
                        .map(Path::new)
                        .collect(Collectors.toUnmodifiableList())
                        .toArray(new Path[] {}))
                .build();
        return executionEnvironment.fromSource(
            fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
      default:
        throw new RuntimeException();
    }
  }
}
