package com.octopus.operators.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.googlecode.aviator.AviatorEvaluator;
import com.octopus.operators.spark.declare.ETLDeclare;
import com.octopus.operators.spark.declare.common.ColumnDesc;
import com.octopus.operators.spark.declare.common.FieldType;
import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.common.TransformType;
import com.octopus.operators.spark.declare.sink.CSVSinkDeclare;
import com.octopus.operators.spark.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import com.octopus.operators.spark.declare.sink.SinkDeclare;
import com.octopus.operators.spark.declare.source.CSVSourceDeclare.CSVSourceOptions;
import com.octopus.operators.spark.declare.source.SourceDeclare;
import com.octopus.operators.spark.declare.source.SourceOptions;
import com.octopus.operators.spark.declare.transform.SparkSQLTransformDeclare;
import com.octopus.operators.spark.declare.transform.SparkSQLTransformDeclare.SparkSQLTransformOptions;
import com.octopus.operators.spark.declare.transform.TransformDeclare;
import com.octopus.operators.spark.utils.SparkOperatorUtils;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class DeclareTests {

  @Test
  public void createETLDeclare() {
    String path =
        DeclareTests.class.getClassLoader().getResource("etl-declare-example.yaml").getPath();
    ETLDeclare config = SparkOperatorUtils.getConfig(path, ETLDeclare.class);
    assertNotNull(config);
    List<SourceDeclare<?>> sources = config.getSources();
    assertTrue(CollectionUtils.isNotEmpty(sources));
    assertEquals(1, sources.size());
    SourceDeclare<?> sourceDeclare = sources.iterator().next();
    SourceType sourceType = sourceDeclare.getType();
    assertEquals(SourceType.csv, sourceType);
    SourceOptions options = sourceDeclare.getOptions();
    assertTrue(options instanceof CSVSourceOptions);
    CSVSourceOptions csvSourceOptions = (CSVSourceOptions) options;
    String dateFormat = csvSourceOptions.getDateFormat();
    assertEquals("yyyy-MM-dd", dateFormat);
    String dateTimeFormat = csvSourceOptions.getDateTimeFormat();
    assertEquals("yyyy-MM-dd HH:mm:ss.SSS", dateTimeFormat);
    List<ColumnDesc> schemas = csvSourceOptions.getSchemas();
    assertEquals(3, schemas.size());

    ColumnDesc columnDesc = schemas.get(0);
    assertEquals("id", columnDesc.getName());
    assertEquals("user_id", columnDesc.getAlias());
    Assertions.assertEquals(FieldType.Integer, columnDesc.getType());
    assertTrue(columnDesc.isPrimaryKey());
    assertFalse(columnDesc.isNullable());

    String sourceOutput = sourceDeclare.getOutput();

    List<TransformDeclare<?>> transformDeclares = config.getTransforms();
    assertTrue(CollectionUtils.isNotEmpty(transformDeclares));
    assertEquals(1, transformDeclares.size());
    TransformDeclare<?> transformDeclare = transformDeclares.iterator().next();
    TransformType transformType = transformDeclare.getType();
    assertEquals(TransformType.sparkSQL, transformType);
    SparkSQLTransformDeclare sparkSQLTransformDeclare = (SparkSQLTransformDeclare) transformDeclare;
    SparkSQLTransformOptions sparkSQLOptions = sparkSQLTransformDeclare.getOptions();
    String sql = sparkSQLOptions.getSql();
    assertEquals("select *, (FLOOR(RAND() * 100) - 10) as age from user_temp", sql);

    Map<String, String> transformInput = sparkSQLTransformDeclare.getInput();
    assertTrue(transformInput.containsKey(sourceOutput));
    String sparkSQLOutput = sparkSQLTransformDeclare.getOutput();

    List<SinkDeclare<?>> sinkDeclares = config.getSinks();
    assertTrue(CollectionUtils.isNotEmpty(sinkDeclares));
    assertEquals(1, sinkDeclares.size());
    SinkDeclare<?> sinkDeclare = sinkDeclares.iterator().next();
    SinkType sinkType = sinkDeclare.getType();
    assertEquals(SinkType.csv, sinkType);
    CSVSinkDeclare csvSinkDeclare = (CSVSinkDeclare) sinkDeclare;
    CSVSinkOptions csvSinkOptions = csvSinkDeclare.getOptions();
    String sinkDateFormat = csvSinkOptions.getDateFormat();
    assertEquals("yyyy-MM-dd", sinkDateFormat);

    assertEquals(sparkSQLOutput, csvSinkDeclare.getInput());
  }

  @Test
  public void test() {
    String expression = "age_lt_0_count<=0";
    boolean res = (boolean) AviatorEvaluator.execute(expression, Map.of("age_lt_0_count", 121));
    assertFalse(res);
  }
}
