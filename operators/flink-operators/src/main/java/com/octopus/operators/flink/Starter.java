package com.octopus.operators.flink;

import com.google.common.io.Resources;
import java.net.URL;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.jackson.JacksonMapperFactory;

public class Starter {

  public static void main(String[] args) throws Exception {
    URL resource = Resources.getResource("user.csv");
    StreamExecutionEnvironment streamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    FileSource<Map> fileSource =
        FileSource.forRecordStreamFormat(
                CsvReaderFormat.forSchema(
                    JacksonMapperFactory.createCsvMapper().schemaWithHeader(),
                    TypeInformation.of(Map.class)),
                new Path(resource.getPath()))
            .build();
    DataStreamSource<Map> streamSource =
        streamExecutionEnvironment.fromSource(
            fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
    //    streamSource.setParallelism(3);
    streamSource.print();
    System.out.println(streamExecutionEnvironment.getExecutionPlan());
    streamExecutionEnvironment.execute("file source");
    ExecutionConfig config = streamExecutionEnvironment.getConfig();
    System.out.println(config);
    streamExecutionEnvironment.close();
  }

  public void execute(String configBase64) {}
}
