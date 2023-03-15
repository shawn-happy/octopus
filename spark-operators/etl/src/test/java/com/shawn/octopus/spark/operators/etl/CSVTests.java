package com.shawn.octopus.spark.operators.etl;

import com.shawn.octopus.spark.operators.common.ColumnDesc;
import com.shawn.octopus.spark.operators.common.declare.sink.CSVSinkDeclare;
import com.shawn.octopus.spark.operators.common.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare.CSVSourceOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.SparkSQLTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.SparkSQLTransformDeclare.SparkSQLTransformOptions;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CSVTests {

  @Test
  public void testCSVSource() {
    String path =
        Thread.currentThread().getContextClassLoader().getResource("csv_source.yaml").getPath();
    CSVSourceDeclare config = SparkOperatorUtils.getConfig(path, CSVSourceDeclare.class);
    Assertions.assertNotNull(config);
    CSVSourceOptions sourceOptions = config.getOptions();
    Assertions.assertNotNull(sourceOptions);
    Assertions.assertEquals("csv-test", sourceOptions.getOutput());
    List<ColumnDesc> schemas = sourceOptions.getSchemas();
    Assertions.assertEquals(3, schemas.size());
    ColumnDesc columnDesc = schemas.get(2);
    Assertions.assertEquals("create_time", columnDesc.getAlias());
  }

  @Test
  public void testCSVSink() {
    String path =
        Thread.currentThread().getContextClassLoader().getResource("csv_sink.yaml").getPath();
    CSVSinkDeclare config = SparkOperatorUtils.getConfig(path, CSVSinkDeclare.class);
    Assertions.assertNotNull(config);
    CSVSinkOptions csvSinkOptions = config.getOptions();
    Assertions.assertNotNull(csvSinkOptions);
    Assertions.assertEquals("csv-input", csvSinkOptions.getInput());
    Assertions.assertEquals("path", csvSinkOptions.getPath());
  }

  @Test
  public void testCSVTransform() {
    String path =
        Thread.currentThread().getContextClassLoader().getResource("csv_transform.yaml").getPath();
    SparkSQLTransformDeclare config =
        SparkOperatorUtils.getConfig(path, SparkSQLTransformDeclare.class);
    Assertions.assertNotNull(config);
    SparkSQLTransformOptions options = config.getOptions();
    Assertions.assertNotNull(options);
    Map<String, String> input = options.getInput();
    Assertions.assertNotNull(input);
    Assertions.assertEquals(2, input.size());
    Assertions.assertEquals("transform-output", options.getOutput());
    Assertions.assertEquals("select * from input_2", options.getSql());
  }
}
