package com.octopus.operators.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.io.Resources;
import com.octopus.operators.flink.declare.sink.ConsoleSinkDeclare;
import com.octopus.operators.flink.declare.source.CSVSourceDeclare;
import com.octopus.operators.flink.declare.transform.SQLTransformDeclare;
import com.octopus.operators.flink.declare.transform.TransformDeclare;
import com.octopus.operators.flink.runtime.FlinkRuntimeConfig;
import com.octopus.operators.flink.runtime.FlinkRuntimeEnvironment;
import com.octopus.operators.flink.runtime.sink.ConsoleSink;
import com.octopus.operators.flink.runtime.source.CSVFileSource;
import com.octopus.operators.flink.runtime.transform.SQLTransform;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// @Disabled
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
    FlinkRuntimeEnvironment flinkRuntimeEnvironment =
        FlinkRuntimeEnvironment.getFlinkRuntimeEnvironment();
    CSVSourceDeclare sourceDeclare = (CSVSourceDeclare) flinkRuntimeConfig.getSources().get(0);
    CSVFileSource source = new CSVFileSource(flinkRuntimeEnvironment, sourceDeclare);
    DataStream<Row> reader = source.reader();
    TransformDeclare<?> transformDeclare = flinkRuntimeConfig.getTransforms().get(0);
    SQLTransform sqlTransform =
        new SQLTransform(flinkRuntimeEnvironment, (SQLTransformDeclare) transformDeclare);
    DataStream<Row> process =
        sqlTransform.process(
            Map.of(transformDeclare.getInput().get(sourceDeclare.getOutput()), reader));

    ConsoleSink sink =
        new ConsoleSink(
            flinkRuntimeEnvironment, (ConsoleSinkDeclare) flinkRuntimeConfig.getSinks().get(0));
    sink.writer(process);
    flinkRuntimeEnvironment.getStreamExecutionEnvironment().execute(flinkRuntimeConfig.getName());
    flinkRuntimeEnvironment.getStreamExecutionEnvironment().close();
  }
}
