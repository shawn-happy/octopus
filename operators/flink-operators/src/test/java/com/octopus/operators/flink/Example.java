package com.octopus.operators.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.io.Resources;
import com.octopus.operators.flink.declare.sink.ConsoleSinkDeclare;
import com.octopus.operators.flink.declare.source.CSVSourceDeclare;
import com.octopus.operators.flink.runtime.FlinkRuntimeConfig;
import com.octopus.operators.flink.runtime.sink.ConsoleSink;
import com.octopus.operators.flink.runtime.source.CSVFileSource;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class Example {

  private static final ObjectMapper objectMapperYaml =
      new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

  @Test
  public void simpleTest() throws Exception {
    FlinkRuntimeConfig flinkRuntimeConfig =
        objectMapperYaml.readValue(
            IOUtils.toString(Resources.getResource("simple.yaml"), Charset.defaultCharset()),
            FlinkRuntimeConfig.class);
    Assertions.assertNotNull(flinkRuntimeConfig);
    CSVSourceDeclare sourceDeclare = (CSVSourceDeclare) flinkRuntimeConfig.getSources().get(0);
    CSVFileSource source = new CSVFileSource(sourceDeclare);
    StreamExecutionEnvironment executionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<?> reader = source.reader(executionEnvironment);

    ConsoleSink sink = new ConsoleSink((ConsoleSinkDeclare) flinkRuntimeConfig.getSinks().get(0));
    sink.writer(executionEnvironment, reader);
    executionEnvironment.execute("job name");
    executionEnvironment.close();
  }
}
