package io.github.shawn.octopus.fluxus.engine.connector.transform.expression;

import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExpressionTransformTests {

  @Test
  void expressionTest() {
    ExpressionTransformConfig config =
        ExpressionTransformConfig.builder()
            .options(
                ExpressionTransformConfig.ExpressionOptions.builder()
                    .expressionFields(
                        new ExpressionTransformConfig.ExpressionField[] {
                          ExpressionTransformConfig.ExpressionField.builder()
                              .expression("name != nil ? ('hello, ' + name):'who are u?'")
                              .destination("out_name")
                              .type("string")
                              .build(),
                          ExpressionTransformConfig.ExpressionField.builder()
                              .expression("name != nil ? ('hello1, ' + name):'who are u?'")
                              .destination("out_name1")
                              .type("string")
                              .build()
                        })
                    .build())
            .build();
    SourceRowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name"})
            .fieldTypes(new DataWorkflowFieldType[] {BasicFieldType.INT, BasicFieldType.STRING})
            .values(new Object[] {1, "dsx"})
            .build();
    RowRecord transform = getRowRecord(config);
    System.out.println(transform);
    Assertions.assertTrue(true);
  }

  private static RowRecord getRowRecord(ExpressionTransformConfig config) {
    String[] fieldNames = {"id", "name"};
    DataWorkflowFieldType[] fieldTypes =
        new DataWorkflowFieldType[] {BasicFieldType.INT, BasicFieldType.STRING};
    List<Object[]> values =
        new LinkedList<>(Arrays.asList(new Object[] {1, "dsx"}, new Object[] {2, "lisi"}));
    TransformRowRecord record = new TransformRowRecord(fieldNames, fieldTypes);
    record.addRecords(values);

    ExpressionTransform expressionTransform = new ExpressionTransform(config);
    expressionTransform.init();
    expressionTransform.begin();
    RowRecord transform = expressionTransform.transform(record);
    System.out.println(transform);
    Assertions.assertTrue(true);
    return expressionTransform.transform(record);
  }

  @Test
  void expressionTest1() {
    ExpressionTransformConfig config =
        ExpressionTransformConfig.builder()
            .options(
                ExpressionTransformConfig.ExpressionOptions.builder()
                    .expressionFields(
                        new ExpressionTransformConfig.ExpressionField[] {
                          ExpressionTransformConfig.ExpressionField.builder()
                              .expression("date.format(e1,create_time)")
                              .destination("out_time")
                              .type("string")
                              .build()
                        })
                    .build())
            .build();
    SourceRowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"e1", "create_time"})
            .fieldTypes(new DataWorkflowFieldType[] {BasicFieldType.STRING, BasicFieldType.STRING})
            .values(new Object[] {"yyyy-MM-dd", "2024-02-15 14:55:50"})
            .build();
    ExpressionTransform expressionTransform = new ExpressionTransform(config);
    expressionTransform.init();
    expressionTransform.begin();
    RowRecord transform = expressionTransform.transform(record);
    System.out.println(transform);
    Assertions.assertTrue(true);
  }
}
