package com.octopus.operators.flink.runtime.source;

import com.octopus.operators.flink.declare.source.CSVSourceDeclare;
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

public class CSVFileSource implements Source<CSVSourceDeclare> {

  private final CSVSourceDeclare declare;

  public CSVFileSource(CSVSourceDeclare declare) {
    this.declare = declare;
  }

  @Override
  public DataStream<?> reader(StreamExecutionEnvironment executionEnvironment) throws Exception {
    var options = declare.getOptions();
    var paths =
        Arrays.stream(options.getPaths())
            .map(Path::new)
            .collect(Collectors.toUnmodifiableList())
            .toArray(new Path[] {});
    var fileSource =
        FileSource.forRecordStreamFormat(
                CsvReaderFormat.forSchema(
                    JacksonMapperFactory.createCsvMapper().schemaWithHeader(),
                    TypeInformation.of(Map.class)),
                paths)
            .build();
    return executionEnvironment.fromSource(
        fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
  }
}
