package com.octopus.operators.engine.table.type;

import static com.octopus.operators.engine.table.type.RowDataTypeParse.parseArrayDataType;
import static com.octopus.operators.engine.table.type.RowDataTypeParse.parseBasicDataType;
import static com.octopus.operators.engine.table.type.RowDataTypeParse.parseMapDataType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.octopus.operators.engine.exception.CommonExceptionConstant;
import com.octopus.operators.engine.exception.EngineException;
import org.junit.jupiter.api.Test;

public class RowDataTypeParseTests {

  @Test
  public void testParseBasicDataType() {
    RowDataType rowDataType = parseBasicDataType(FieldType.STRING);
    assertEquals(PrimitiveDataType.STRING, rowDataType);

    rowDataType = parseBasicDataType("long");
    assertEquals(PrimitiveDataType.BIGINT, rowDataType);

    rowDataType = parseBasicDataType("varchar");
    assertEquals(PrimitiveDataType.STRING, rowDataType);

    rowDataType = parseBasicDataType("bigint");
    assertEquals(PrimitiveDataType.BIGINT, rowDataType);

    rowDataType = parseBasicDataType("array");
    assertNull(rowDataType);

    rowDataType = parseBasicDataType("map");
    assertNull(rowDataType);

    rowDataType = parseBasicDataType("decimal");
    assertNull(rowDataType);

    rowDataType = parseBasicDataType("int");
    assertEquals(PrimitiveDataType.INT, rowDataType);

    EngineException engineException =
        assertThrows(EngineException.class, () -> parseBasicDataType("bit"));
    assertEquals(CommonExceptionConstant.unsupportedDataType("bit"), engineException.getMessage());

    rowDataType = parseBasicDataType(FieldType.DATE);
    assertEquals(DateDataType.DATE_TYPE, rowDataType);

    rowDataType = parseBasicDataType(FieldType.TIMESTAMP);
    assertEquals(DateDataType.DATE_TIME_TYPE, rowDataType);

    rowDataType = parseBasicDataType(FieldType.TIME);
    assertEquals(DateDataType.TIME_TYPE, rowDataType);
  }

  @Test
  public void testArrayDataType() {
    String fieldType = "array<int>";
    RowDataType rowDataType = parseArrayDataType(fieldType);
    assertEquals(ArrayDataType.INT_ARRAY, rowDataType);

    fieldType = "array<String>";
    rowDataType = parseArrayDataType(fieldType);
    assertEquals(ArrayDataType.STRING_ARRAY, rowDataType);

    fieldType = "array<varchar>";
    rowDataType = parseArrayDataType(fieldType);
    assertEquals(ArrayDataType.STRING_ARRAY, rowDataType);

    fieldType = "array<long>";
    rowDataType = parseArrayDataType(fieldType);
    assertEquals(ArrayDataType.BIGINT_ARRAY, rowDataType);

    fieldType = "array<bigint>";
    rowDataType = parseArrayDataType(fieldType);
    assertEquals(ArrayDataType.BIGINT_ARRAY, rowDataType);

    fieldType = "array<boolean>";
    rowDataType = parseArrayDataType(fieldType);
    assertEquals(ArrayDataType.BOOLEAN_ARRAY, rowDataType);

    EngineException engineException =
        assertThrows(EngineException.class, () -> parseArrayDataType("array<date>"));
    assertEquals(
        CommonExceptionConstant.unsupportedDataType("array<date>"), engineException.getMessage());
  }

  @Test
  public void testMapDataType() {
    String fieldType = "map<int, int>";
    RowDataType rowDataType = parseMapDataType(fieldType);
    assertEquals("map<int, int>", rowDataType.toString());

    fieldType = "map<String, int>";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<string, int>", rowDataType.toString());

    fieldType = "Map<varchar, int>";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<string, int>", rowDataType.toString());

    fieldType = "map<long, Int>";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<bigint, int>", rowDataType.toString());

    fieldType = "map<BIGINT, Int  >";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<bigint, int>", rowDataType.toString());

    fieldType = "map<decimal<1,1>, int>";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<decimal<1, 1>, int>", rowDataType.toString());

    fieldType = "map<decimal<1,1>, decimal< 2,  2> >";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<decimal<1, 1>, decimal<2, 2>>", rowDataType.toString());

    fieldType = "map< map< String ,  int>  , map < String ,  array<int> > >";
    rowDataType = parseMapDataType(fieldType);
    assertEquals("map<map<string, int>, map<string, array<int>>>", rowDataType.toString());
  }
}
