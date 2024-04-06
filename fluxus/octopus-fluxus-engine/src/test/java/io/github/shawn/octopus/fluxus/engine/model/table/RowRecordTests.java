package io.github.shawn.octopus.fluxus.engine.model.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.model.type.DateTimeFieldType;
import org.junit.jupiter.api.Test;

public class RowRecordTests {

  @Test
  public void testAddColumn() {
    TransformRowRecord rowRecord =
        new TransformRowRecord(
            new String[] {"a"}, new DataWorkflowFieldType[] {BasicFieldType.STRING});
    rowRecord.addColumn("b", BasicFieldType.STRING);
    String[] fieldNames = rowRecord.getFieldNames();
    DataWorkflowFieldType[] fieldTypes = rowRecord.getFieldTypes();
    assertEquals(2, fieldNames.length);
    assertEquals(2, fieldTypes.length);
    assertEquals("b", fieldNames[1]);
  }

  @Test
  public void testAddColumns() {
    TransformRowRecord rowRecord =
        new TransformRowRecord(
            new String[] {"a"}, new DataWorkflowFieldType[] {BasicFieldType.STRING});
    rowRecord.addColumn("b", BasicFieldType.STRING);
    rowRecord.addColumn("c", BasicFieldType.INT);
    String[] fieldNames = rowRecord.getFieldNames();
    DataWorkflowFieldType[] fieldTypes = rowRecord.getFieldTypes();
    assertEquals(3, fieldNames.length);
    assertEquals(3, fieldTypes.length);
    assertEquals("b", fieldNames[1]);
    assertEquals("c", fieldNames[2]);
    assertEquals(BasicFieldType.INT, fieldTypes[2]);

    rowRecord.addColumns(
        new String[] {"d", "e", "f"},
        new DataWorkflowFieldType[] {
          BasicFieldType.FLOAT, DateTimeFieldType.DATE_TIME_TYPE, DateTimeFieldType.DATE_TIME_TYPE
        });

    fieldNames = rowRecord.getFieldNames();
    fieldTypes = rowRecord.getFieldTypes();
    assertEquals(6, fieldNames.length);
    assertEquals(6, fieldTypes.length);
    assertEquals("b", fieldNames[1]);
    assertEquals("c", fieldNames[2]);
    assertEquals("e", fieldNames[4]);
    assertEquals("f", fieldNames[5]);
    assertEquals(BasicFieldType.INT, fieldTypes[2]);
    assertEquals(DateTimeFieldType.DATE_TIME_TYPE, fieldTypes[4]);
    assertEquals(DateTimeFieldType.DATE_TIME_TYPE, fieldTypes[5]);

    assertThrows(
        DataWorkflowException.class, () -> rowRecord.addColumn("b", BasicFieldType.STRING));

    assertThrows(
        DataWorkflowException.class,
        () ->
            rowRecord.addColumns(
                new String[] {"d", "e"},
                new DataWorkflowFieldType[] {
                  BasicFieldType.FLOAT, DateTimeFieldType.DATE_TIME_TYPE
                }));

    assertThrows(
        DataWorkflowException.class,
        () ->
            rowRecord.addColumns(
                new String[] {"d"},
                new DataWorkflowFieldType[] {
                  BasicFieldType.FLOAT, DateTimeFieldType.DATE_TIME_TYPE
                }));
  }
}
